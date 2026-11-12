using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace LoadTest.Client;

/// <summary>
/// Stops and starts the nodes of a Docker-hosted KurrentDB cluster, and reads cluster state from the
/// gossip endpoint. Nodes are identified by container name; each container's published port for 2113
/// is used to match gossip members (which advertise that port to clients) to containers.
/// </summary>
internal sealed class KurrentNodeManager(IReadOnlyList<string> containers) : IDisposable
{
    private readonly HttpClient http = new() { Timeout = TimeSpan.FromSeconds(2) };
    private readonly Dictionary<string, int> ports = [];

    public IReadOnlyList<string> Containers => containers;

    public async Task InitializeAsync()
    {
        foreach (var container in containers)
        {
            var (exitCode, output) = await DockerAsync("port", container, "2113/tcp");
            var port = output.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(line => line.Trim().Split(':').Last()).FirstOrDefault();
            if (exitCode != 0 || !int.TryParse(port, out var parsed))
            {
                throw new InvalidOperationException($"Could not find the published 2113 port of container '{container}' (docker port: {output.Trim()}). Pass the cluster's container names with --kurrent-nodes.");
            }

            ports[container] = parsed;
        }
    }

    /// <summary>Returns each container's gossip state (Leader, Follower, ...), or "down" when not alive.</summary>
    public async Task<IReadOnlyDictionary<string, string>> GetStatesAsync()
    {
        var states = containers.ToDictionary(c => c, _ => "down");
        foreach (var port in ports.Values)
        {
            try
            {
                var gossip = await http.GetFromJsonAsync<Gossip>($"http://localhost:{port}/gossip");
                foreach (var member in gossip?.Members ?? [])
                {
                    if (member.IsAlive && ports.FirstOrDefault(p => p.Value == member.HttpEndPointPort).Key is { } container)
                    {
                        states[container] = member.State;
                    }
                }

                return states;
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                // That node is down; ask the next one.
            }
        }

        return states;
    }

    public async Task<string?> GetLeaderAsync()
        => (await GetStatesAsync()).FirstOrDefault(s => s.Value == "Leader").Key;

    /// <summary>Containers in rolling-update order: followers first and the current leader last, or the reverse.</summary>
    public async Task<IReadOnlyList<string>> GetRollingOrderAsync(string order)
    {
        if (order == "listed")
        {
            return containers;
        }

        var leader = await GetLeaderAsync();
        var followers = containers.Where(c => c != leader).ToList();
        return leader is null ? containers : order == "leader-first" ? [leader, .. followers] : [.. followers, leader];
    }

    public async Task StopAsync(string container, bool graceful)
    {
        var (exitCode, output) = graceful ? await DockerAsync("stop", "--time", "30", container) : await DockerAsync("kill", container);
        if (exitCode != 0)
        {
            throw new InvalidOperationException($"docker {(graceful ? "stop" : "kill")} {container} failed: {output.Trim()}");
        }
    }

    /// <summary>Starts a container and waits until it is back in the cluster as a leader or follower.</summary>
    public async Task StartAsync(string container, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var (exitCode, output) = await DockerAsync("start", container);
        if (exitCode != 0)
        {
            throw new InvalidOperationException($"docker start {container} failed: {output.Trim()}");
        }

        var watch = Stopwatch.StartNew();
        while (watch.Elapsed < timeout && !cancellationToken.IsCancellationRequested)
        {
            if ((await GetStatesAsync()).TryGetValue(container, out var state) && state is "Leader" or "Follower")
            {
                return;
            }

            await Task.Delay(1000, cancellationToken).ContinueWith(_ => { }, TaskScheduler.Default);
        }

        throw new TimeoutException($"{container} did not rejoin the cluster as leader or follower within {timeout.TotalSeconds:N0}s.");
    }

    public void Dispose() => http.Dispose();

    private static async Task<(int ExitCode, string Output)> DockerAsync(params string[] args)
    {
        var startInfo = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        foreach (var arg in args)
        {
            startInfo.ArgumentList.Add(arg);
        }

        using var process = Process.Start(startInfo)!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        return (process.ExitCode, await stdout + await stderr);
    }

    private sealed record Gossip([property: JsonPropertyName("members")] GossipMember[] Members);

    private sealed record GossipMember(
        [property: JsonPropertyName("state")] string State,
        [property: JsonPropertyName("isAlive")] bool IsAlive,
        [property: JsonPropertyName("httpEndPointPort")] int HttpEndPointPort);
}
