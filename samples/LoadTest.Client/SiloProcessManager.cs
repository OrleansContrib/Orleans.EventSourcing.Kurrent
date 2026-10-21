using System.Diagnostics;
using System.Text.RegularExpressions;

namespace LoadTest.Client;

/// <summary>
/// Runs LoadTest.Silo instances as child processes so a scenario can stop, kill and replace them.
/// Each silo is started with stdin as a control channel (see LoadTest.Silo's Program.cs): writing
/// "stop" shuts it down gracefully, and if this process dies the pipe closes and the silo stops too.
/// Silo output goes to one log file per silo under <see cref="LogDirectory"/>.
/// </summary>
internal sealed partial class SiloProcessManager(string siloDllPath, string connectionString, string logDirectory) : IAsyncDisposable
{
    private readonly List<ManagedSilo> silos = [];
    private int nextId;

    public string LogDirectory => logDirectory;

    public IReadOnlyList<ManagedSilo> Running
    {
        get
        {
            lock (silos)
            {
                return silos.Where(s => !s.Process.HasExited).ToArray();
            }
        }
    }

    /// <summary>The LoadTest.Silo build next to this client's build (same configuration and target framework).</summary>
    public static string DefaultSiloPath()
    {
        var baseDirectory = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        var targetFramework = baseDirectory.Name;
        var configuration = baseDirectory.Parent!.Name;
        return Path.GetFullPath(Path.Combine(baseDirectory.FullName, "..", "..", "..", "..", "LoadTest.Silo", "bin", configuration, targetFramework, "LoadTest.Silo.dll"));
    }

    public async Task<ManagedSilo> StartAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(siloDllPath))
        {
            throw new FileNotFoundException($"Silo build not found at {siloDllPath}. Build samples/LoadTest.Silo in the same configuration, or pass --silo-path.", siloDllPath);
        }

        Directory.CreateDirectory(logDirectory);
        var id = Interlocked.Increment(ref nextId);
        var logPath = Path.Combine(logDirectory, $"silo-{id}.log");
        var startInfo = new ProcessStartInfo("dotnet")
        {
            ArgumentList = { siloDllPath },
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = Path.GetDirectoryName(siloDllPath),
        };
        startInfo.Environment["KURRENT_CONNECTION_STRING"] = connectionString;
        startInfo.Environment["LOADTEST_STDIN_CONTROL"] = "1";

        var silo = new ManagedSilo(id, logPath, Process.Start(startInfo)!);
        lock (silos)
        {
            silos.Add(silo);
        }

        silo.BeginCapture();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(2));
        try
        {
            await silo.Ready.WaitAsync(timeout.Token);
        }
        catch (Exception ex)
        {
            silo.Kill();
            var tail = string.Join(Environment.NewLine, silo.TailLines(8).Select(line => "    " + line));
            throw new InvalidOperationException($"Silo {id} failed to start ({ex.GetType().Name}). Last output from {logPath}:{Environment.NewLine}{tail}", ex);
        }

        return silo;
    }

    /// <summary>Stops a silo gracefully (falling back to a kill after <paramref name="timeout"/>) or kills it.</summary>
    public static async Task StopAsync(ManagedSilo silo, bool graceful, TimeSpan timeout)
    {
        if (graceful)
        {
            try
            {
                await silo.Process.StandardInput.WriteLineAsync("stop");
                await silo.Process.StandardInput.FlushAsync();
                await silo.Process.WaitForExitAsync().WaitAsync(timeout);
                return;
            }
            catch (TimeoutException)
            {
                Console.WriteLine($"  silo {silo.Id} did not stop within {timeout.TotalSeconds:N0}s; killing it");
            }
            catch (IOException)
            {
                // The process already exited and closed its stdin.
            }
        }

        silo.Kill();
        await silo.Process.WaitForExitAsync();
    }

    public async ValueTask DisposeAsync()
    {
        var running = Running;
        if (running.Count > 0)
        {
            Console.WriteLine($"Stopping {running.Count} managed silo(s)...");
            await Task.WhenAll(running.Select(s => StopAsync(s, graceful: true, TimeSpan.FromSeconds(60))));
        }

        foreach (var silo in silos)
        {
            silo.Dispose();
        }
    }

    internal sealed partial class ManagedSilo(int id, string logPath, Process process) : IDisposable
    {
        private readonly TaskCompletionSource ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Queue<string> tail = new();
        private readonly StreamWriter log = new(logPath) { AutoFlush = true };

        public int Id { get; } = id;

        public Process Process { get; } = process;

        /// <summary>The silo address the process reported once it was running.</summary>
        public string? Address { get; private set; }

        public Task Ready => ready.Task;

        public override string ToString() => $"silo {Id} ({Address ?? "starting"})";

        public void BeginCapture()
        {
            Process.OutputDataReceived += (_, e) => OnLine(e.Data);
            Process.ErrorDataReceived += (_, e) => OnLine(e.Data);
            Process.Exited += (_, _) => ready.TrySetException(new InvalidOperationException($"exited with code {Process.ExitCode}"));
            Process.EnableRaisingEvents = true;
            Process.BeginOutputReadLine();
            Process.BeginErrorReadLine();
        }

        public IReadOnlyList<string> TailLines(int count)
        {
            lock (tail)
            {
                return tail.TakeLast(count).ToArray();
            }
        }

        public void Kill()
        {
            try
            {
                Process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
                // Already exited.
            }
        }

        public void Dispose()
        {
            Process.Dispose();
            lock (log)
            {
                log.Dispose();
            }
        }

        private void OnLine(string? line)
        {
            if (line is null)
            {
                return;
            }

            lock (log)
            {
                if (log.BaseStream.CanWrite)
                {
                    log.WriteLine(line);
                }
            }

            lock (tail)
            {
                tail.Enqueue(line);
                if (tail.Count > 50)
                {
                    tail.Dequeue();
                }
            }

            if (RunningPattern().Match(line) is { Success: true } match)
            {
                Address = match.Groups[1].Value;
                ready.TrySetResult();
            }
        }

        [GeneratedRegex(@"silo is running at (\S+)\. Press")]
        private static partial Regex RunningPattern();
    }
}
