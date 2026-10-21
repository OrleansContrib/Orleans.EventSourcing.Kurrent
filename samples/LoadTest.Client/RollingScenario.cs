using KurrentDB.Client;
using System.Diagnostics;

namespace LoadTest.Client;

/// <summary>
/// Simulates a rolling update of the Orleans silos and the KurrentDB nodes under steady load, then
/// verifies nothing was lost.
///
/// The client runs the silos itself (<see cref="SiloProcessManager"/>) and drives the KurrentDB
/// containers through Docker (<see cref="KurrentNodeManager"/>). While a constant rate of event
/// appends and a set of reminders run, it:
///   1. takes each silo out in turn and brings up a replacement (stop then start, or start then stop with --surge),
///      waiting for the cluster to converge on the new membership each time;
///   2. takes each Kurrent node out in turn (followers first, leader last by default), leaves it down for
///      --node-down seconds, restarts it and waits for it to rejoin as leader or follower.
/// A timeline row is printed every --report-interval seconds, followed by per-phase throughput, failures,
/// latency and the longest gap with no successful write. Finally the event logs are checked against
/// acknowledged appends and the reminders are checked for any that never fired or stopped firing.
/// </summary>
internal static class RollingScenario
{
    public static async Task<int> RunAsync(IClusterClient client, CommandLine args, SiloProcessManager? silos, int siloCount, string connectionString, CancellationToken cancellationToken)
    {
        var plan = args.GetString("plan", "both")!.ToLowerInvariant();
        var rollSilos = plan is "both" or "silos";
        var rollKurrent = plan is "both" or "kurrent";
        var gracefulSilos = args.GetString("silo-stop", "graceful") != "kill";
        var gracefulKurrent = args.GetString("kurrent-stop", "graceful") != "kill";
        var surge = args.Has("surge");
        var settle = TimeSpan.FromSeconds(args.GetInt("settle", 30));
        var nodeDown = TimeSpan.FromSeconds(args.GetInt("node-down", 30));
        var rate = args.GetDouble("rate", 200);
        var reportInterval = TimeSpan.FromSeconds(args.GetInt("report-interval", 5));
        var reminderCount = args.GetInt("reminders", 200);
        var period = TimeSpan.FromSeconds(args.GetInt("period", 60));
        var nodes = args.GetString("kurrent-nodes", "kurrentdb1,kurrentdb2,kurrentdb3")!.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var nodeOrder = args.GetString("kurrent-order", "followers-first")!;

        if (plan is not ("both" or "silos" or "kurrent" or "none"))
        {
            throw new ArgumentException($"Unknown --plan '{plan}'. Expected both, silos, kurrent or none.");
        }

        if (rollSilos && silos is null)
        {
            throw new ArgumentException("Rolling silos needs the client to manage them; drop --external-silos or use --plan kurrent.");
        }

        if (rollKurrent && KurrentDBClientSettings.Create(connectionString).ConnectivitySettings.GossipSeeds.Length < 2)
        {
            throw new ArgumentException($"KURRENT_CONNECTION_STRING ({connectionString}) names a single node, so clients cannot fail over when it is stopped. "
                                      + "List every node, e.g. esdb://localhost:2113,localhost:2213,localhost:2313?tls=false");
        }

        using var kurrent = rollKurrent ? new KurrentNodeManager(nodes) : null;
        if (kurrent is not null)
        {
            await kurrent.InitializeAsync();
        }

        Console.WriteLine($"Rolling scenario: plan={plan} silos={siloCount}{(silos is null ? " (external)" : "")} silo-stop={(gracefulSilos ? "graceful" : "kill")}{(surge ? " surge" : "")} "
                        + $"kurrent-nodes={(kurrent is null ? "-" : string.Join(',', nodes))} kurrent-stop={(gracefulKurrent ? "graceful" : "kill")} order={nodeOrder} "
                        + $"rate={rate:N0}/s settle={settle.TotalSeconds:N0}s node-down={nodeDown.TotalSeconds:N0}s reminders={reminderCount}");
        if (silos is not null)
        {
            Console.WriteLine($"Silo logs: {silos.LogDirectory}");
        }

        var events = EventWorkload.Create(client, args, defaultGrains: 500);
        Console.WriteLine($"Events: {events}");
        await events.ActivateAsync(cancellationToken);

        using var reminders = new ReminderWorkload(client, period);
        if (reminderCount > 0)
        {
            await reminders.PreloadAsync(reminderCount, 32, cancellationToken);
        }

        var load = new SteadyLoad(rate, args.GetInt("max-in-flight", 10_000), events.AppendAsync);
        var disruptions = new List<DateTime>();
        using var stopLoad = new CancellationTokenSource();
        load.BeginPhase("baseline");
        Console.WriteLine();
        var loadTask = load.RunAsync(reportInterval, () => DescribeClusterAsync(client, kurrent), stopLoad.Token);

        var planFailed = false;
        try
        {
            await Delay(settle, cancellationToken);

            if (rollSilos && !cancellationToken.IsCancellationRequested)
            {
                await RollSilosAsync(client, silos!, load, disruptions, gracefulSilos, surge, settle, cancellationToken);
            }

            if (kurrent is not null && !cancellationToken.IsCancellationRequested)
            {
                await RollKurrentAsync(kurrent, load, disruptions, gracefulKurrent, nodeOrder, nodeDown, settle, cancellationToken);
            }

            // Give reminders long enough after the last disruption to show any that stopped firing.
            var tail = reminderCount > 0 ? TimeSpan.FromTicks(Math.Max(settle.Ticks, (period * 2 + TimeSpan.FromSeconds(15)).Ticks)) : settle;
            load.BeginPhase("final");
            load.Log($"holding steady for {tail.TotalSeconds:N0}s");
            await Delay(tail, cancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            // Still stop the load and verify: what happened before the failure is worth knowing.
            load.Log($"DISRUPTION PLAN FAILED: {ex.Message}");
            planFailed = true;
        }

        stopLoad.Cancel();
        await loadTask;
        load.PrintPhaseSummary();

        var eventsPassed = await events.VerifyAsync();
        var remindersPassed = reminderCount == 0 || reminders.Report(DateTime.UtcNow, disruptions);
        if (reminderCount > 0)
        {
            await reminders.CleanupAsync();
        }

        var failed = load.Phases.Sum(p => p.Window.Failed);
        var succeeded = load.Phases.Sum(p => p.Window.Succeeded);
        var longestGap = load.Phases.Select(p => p.Window.LongestGap).DefaultIfEmpty().Max();
        Console.WriteLine();
        Console.WriteLine($"Availability: {failed:N0} failed of {failed + succeeded:N0} appends ({(failed + succeeded == 0 ? 0 : (double)failed / (failed + succeeded)):P3}), longest gap without a successful append {longestGap.TotalSeconds:N1}s");
        var maxFailed = args.GetInt("max-failed", -1);
        var availabilityPassed = maxFailed < 0 || failed <= maxFailed;
        if (!availabilityPassed)
        {
            Console.WriteLine($"AVAILABILITY FAILED: {failed:N0} failed appends exceeds --max-failed {maxFailed:N0}");
        }

        var passed = eventsPassed && remindersPassed && availabilityPassed && !planFailed;
        Console.WriteLine(passed ? "ROLLING UPDATE PASSED" : "ROLLING UPDATE FAILED");
        return passed ? 0 : 1;
    }

    private static async Task RollSilosAsync(
        IClusterClient client,
        SiloProcessManager silos,
        SteadyLoad load,
        List<DateTime> disruptions,
        bool graceful,
        bool surge,
        TimeSpan settle,
        CancellationToken cancellationToken)
    {
        var originals = silos.Running.ToList();
        for (var i = 0; i < originals.Count && !cancellationToken.IsCancellationRequested; i++)
        {
            var old = originals[i];
            var label = $"silo {i + 1}/{originals.Count}";
            SiloProcessManager.ManagedSilo? replacement = null;

            if (surge)
            {
                load.BeginPhase($"{label}: replacement starting");
                replacement = await StartReplacementAsync();
            }

            load.BeginPhase($"{label}: {(graceful ? "stopping" : "killed")}");
            load.Log($"{(graceful ? "stopping" : "killing")} {old}");
            disruptions.Add(DateTime.UtcNow);
            var watch = Stopwatch.StartNew();
            await SiloProcessManager.StopAsync(old, graceful, TimeSpan.FromSeconds(90));
            load.Log($"{old} exited after {watch.Elapsed.TotalSeconds:N1}s");

            if (!surge)
            {
                load.BeginPhase($"{label}: replacement starting");
                replacement = await StartReplacementAsync();
            }

            watch.Restart();
            await WaitForMembershipAsync(client, replacement!.Address!, old.Address!, cancellationToken);
            load.Log($"membership converged ({replacement} active, {old.Address} gone) after {watch.Elapsed.TotalSeconds:N1}s");

            load.BeginPhase($"{label}: settle");
            await Delay(settle, cancellationToken);
        }

        async Task<SiloProcessManager.ManagedSilo> StartReplacementAsync()
        {
            var watch = Stopwatch.StartNew();
            load.Log("starting replacement silo");
            var silo = await silos.StartAsync(cancellationToken);
            load.Log($"{silo} running after {watch.Elapsed.TotalSeconds:N1}s");
            return silo;
        }
    }

    private static async Task RollKurrentAsync(
        KurrentNodeManager kurrent,
        SteadyLoad load,
        List<DateTime> disruptions,
        bool graceful,
        string order,
        TimeSpan nodeDown,
        TimeSpan settle,
        CancellationToken cancellationToken)
    {
        var sequence = await kurrent.GetRollingOrderAsync(order);
        load.Log($"Kurrent rolling order: {string.Join(" -> ", sequence)} (leader {await kurrent.GetLeaderAsync() ?? "unknown"})");
        for (var i = 0; i < sequence.Count && !cancellationToken.IsCancellationRequested; i++)
        {
            var node = sequence[i];
            var leader = await kurrent.GetLeaderAsync();
            var label = $"{node}{(node == leader ? " (leader)" : "")}";

            load.BeginPhase($"{label}: {(graceful ? "stopping" : "killed")}");
            load.Log($"{(graceful ? "stopping" : "killing")} {label}");
            disruptions.Add(DateTime.UtcNow);
            var watch = Stopwatch.StartNew();
            await kurrent.StopAsync(node, graceful);
            load.Log($"{node} stopped after {watch.Elapsed.TotalSeconds:N1}s");

            load.BeginPhase($"{node}: down");
            await Delay(nodeDown, cancellationToken);

            load.BeginPhase($"{node}: restarting");
            watch.Restart();
            await kurrent.StartAsync(node, TimeSpan.FromMinutes(3), cancellationToken);
            load.Log($"{node} rejoined after {watch.Elapsed.TotalSeconds:N1}s (leader now {await kurrent.GetLeaderAsync() ?? "unknown"})");

            load.BeginPhase($"{node}: settle");
            await Delay(settle, cancellationToken);
        }
    }

    /// <summary>Waits until the replacement silo is active and the old one has left the active membership.</summary>
    private static async Task WaitForMembershipAsync(IClusterClient client, string added, string removed, CancellationToken cancellationToken)
    {
        var watch = Stopwatch.StartNew();
        while (watch.Elapsed < TimeSpan.FromMinutes(3) && !cancellationToken.IsCancellationRequested)
        {
            try
            {
                var active = (await client.GetGrain<IManagementGrain>(0).GetHosts(onlyActive: true)).Keys.Select(s => s.ToString()).ToHashSet();
                if (active.Contains(added) && !active.Contains(removed))
                {
                    return;
                }
            }
            catch (Exception)
            {
                // Membership can be briefly unreadable while silos come and go.
            }

            await Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }

        throw new TimeoutException($"Membership did not converge within 3 minutes (waiting for {added} to be active and {removed} to leave).");
    }

    private static async Task<string> DescribeClusterAsync(IClusterClient client, KurrentNodeManager? kurrent)
    {
        string silos;
        try
        {
            var hosts = await client.GetGrain<IManagementGrain>(0).GetHosts(onlyActive: true).WaitAsync(TimeSpan.FromSeconds(3));
            silos = $"silos {hosts.Count}";
        }
        catch (Exception ex)
        {
            silos = $"silos ? ({ex.GetType().Name})";
        }

        if (kurrent is null)
        {
            return silos;
        }

        var states = await kurrent.GetStatesAsync();
        return $"{silos} | kurrent {string.Join(' ', kurrent.Containers.Select(c => $"{c}={Abbreviate(states[c])}"))}";

        static string Abbreviate(string state) => state switch { "Leader" => "L", "Follower" => "F", "down" => "-", _ => state };
    }

    private static Task Delay(TimeSpan delay, CancellationToken cancellationToken)
        => Task.Delay(delay, cancellationToken).ContinueWith(_ => { }, TaskScheduler.Default);
}
