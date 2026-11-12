using KurrentDB.Client;
using LoadTest.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.EventSourcing.Kurrent.Hosting;

// ---------------------------------------------------------------------------
// LoadTest.Client — drives load against the LoadTest.Silo to validate the
// performance and scalability of the Kurrent providers.
//
// Usage: dotnet run --project samples\LoadTest.Client -c Release -- <scenario> [options]
//
// Ramp options (events, reminders) — an open-loop ramp that starts at --start-rate ops/sec and
// adds --step-rate every --step-seconds, reporting throughput, latency and failures per step.
// Each operation counts towards the step that issued it; a step is judged --settle-seconds after it ends.
//   --start-rate <n>         initial target ops/sec
//   --step-rate <n>          ops/sec added each step
//   --step-seconds <n>       duration of each step
//   --max-steps <n>          number of steps                          (default 20)
//   --max-rate <n>           cap on the target rate
//   --max-in-flight <n>      client-side concurrency cap; excess ops are shed (default 10000)
//   --warmup-seconds <n>     unreported load at --start-rate before step 1 (default 10)
//   --settle-seconds <n>     wait for a step's operations before judging it (default 5)
//   --max-error-rate <f>     stop when a step's failure ratio exceeds this (default 0.01)
//   --max-p99-ms <n>         stop when a step's p99 latency exceeds this (default off)
//   --min-achieved <f>       stop when a step achieves less than this fraction of target (default 0.9)
//   --max-client-lag-ms <n>  stop when the client issues ops this late (p99), i.e. the client is the limit (default 100)
//   --csv <path>             write per-step results to a CSV file
//
// events — event write throughput through the log-consistency provider:
//   --grains <n>             number of event-writer grains            (default 1000)
//   --batch <n>              events raised per append                 (default 1)
//   --payload <bytes>        payload size per event                   (default 64)
//   (ramp defaults: start 200/s, +200/s every 30s)
//
// reminders — Kurrent reminder table registration throughput, then tick delivery and lateness:
//   --preload <n>            register n reminders up front, as fast as possible (default 0)
//   --preload-concurrency <n> concurrent registrations during preload  (default 64)
//   --period <seconds>       reminder period, minimum 60              (default 60)
//   --observe <seconds>      how long to observe ticks after the ramp (default 3 x period)
//   --kill-silo-after <s>    hard-kill a random silo this far into observation (needs >= 2 silos)
//   --no-cleanup             leave the reminders registered afterwards
//   (ramp defaults: start 20/s, +20/s every 30s, 10 steps, stop on any failure; --max-steps 0 skips it)
//
// rolling — rolling update of the silos, then the KurrentDB nodes, under steady load. The client starts
// its own silos and drives the KurrentDB containers through Docker; KURRENT_CONNECTION_STRING must list
// every node, e.g. esdb://localhost:2113,localhost:2213,localhost:2313?tls=false
//   --silos <n>              silos to run                             (default 3)
//   --plan <p>               both | silos | kurrent | none            (default both)
//   --silo-stop <m>          graceful | kill                          (default graceful)
//   --surge                  start each replacement silo before stopping the old one
//   --kurrent-nodes <list>   container names                          (default kurrentdb1,kurrentdb2,kurrentdb3)
//   --kurrent-stop <m>       graceful (docker stop) | kill            (default graceful)
//   --kurrent-order <o>      followers-first | leader-first | listed  (default followers-first)
//   --node-down <seconds>    how long each Kurrent node stays down    (default 30)
//   --settle <seconds>       steady load before, between and after steps (default 30)
//   --rate <n>               event appends per second                 (default 200)
//   --grains/--batch/--payload as for events (default 500 grains)
//   --reminders <n>          reminders kept ticking throughout        (default 200)
//   --max-failed <n>         fail the run if more appends than this fail (default off)
//   --report-interval <s>    timeline row interval                    (default 5)
//   --external-silos         use already-running silos (only --plan kurrent)
//   --silo-path <dll>        LoadTest.Silo.dll to run                 (default: the sibling build)
//   --log-dir <dir>          where silo logs go                       (default loadtest-logs/<timestamp>)
//
// mixed (default) — closed-loop deposits/withdrawals/reads/reminder churn on account grains:
//   --grains <n>             number of event-sourced account grains   (default 10000)
//   --workers <n>            number of concurrent client workers      (default 32)
//   --duration <seconds>     duration of the load phase               (default 90, minimum 90)
//   --reminder-fraction <f>  fraction of grains with an active reminder (default 0.1)
//   --observe-reminders <s>  seconds to wait for reminder ticks after the run (default 0)
// ---------------------------------------------------------------------------

var scenario = args.Length > 0 && !args[0].StartsWith("--", StringComparison.Ordinal) ? args[0].ToLowerInvariant() : "mixed";
var options = new CommandLine(args);

// Connection string for KurrentDB. The special host "kurrentemulator" selects the in-process,
// in-memory Kurrent implementation so the load test runs without external infrastructure.
// Point KURRENT_CONNECTION_STRING at a real KurrentDB instance to measure end-to-end performance.
var connectionString = Environment.GetEnvironmentVariable("KURRENT_CONNECTION_STRING")
                       ?? "esdb://localhost:2113?tls=false";


// Ctrl+C stops the load early but still runs verification and cleanup.
using var cancellation = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    if (!cancellation.IsCancellationRequested)
    {
        e.Cancel = true;
        Console.WriteLine("Stopping... (Ctrl+C again to abort)");
        cancellation.Cancel();
    }
};

// The rolling scenario runs its own silos (unless --external-silos), so they must be up before the client connects.
var siloCount = options.GetInt("silos", 3);
await using var managedSilos = scenario == "rolling" && !options.Has("external-silos")
    ? new SiloProcessManager(
        options.GetString("silo-path") ?? SiloProcessManager.DefaultSiloPath(),
        connectionString,
        Path.GetFullPath(options.GetString("log-dir") ?? Path.Combine("loadtest-logs", DateTime.Now.ToString("yyyyMMdd-HHmmss"))))
    : null;
if (managedSilos is not null)
{
    Console.WriteLine($"Starting {siloCount} silos...");
    for (var i = 0; i < siloCount; i++)
    {
        var silo = await managedSilos.StartAsync(cancellation.Token);
        Console.WriteLine($"  {silo} running");
    }
}

var builder = Host.CreateApplicationBuilder();
builder.Logging.AddFilter("Orleans", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
builder.UseOrleansClient(client => client.UseKurrentClustering(x => x.ClientSettings = KurrentDBClientSettings.Create(connectionString)));

using var host = builder.Build();
await host.StartAsync();
var clusterClient = host.Services.GetRequiredService<IClusterClient>();
Console.WriteLine("Connected.");

var exitCode = scenario switch
{
    "events" => await EventsScenario.RunAsync(clusterClient, options, cancellation.Token),
    "reminders" => await RemindersScenario.RunAsync(clusterClient, options, cancellation.Token),
    "rolling" => await RollingScenario.RunAsync(clusterClient, options, managedSilos, siloCount, connectionString, cancellation.Token),
    "mixed" => await MixedScenario.RunAsync(clusterClient, options),
    _ => throw new ArgumentException($"Unknown scenario '{scenario}'. Expected events, reminders, rolling or mixed."),
};

await host.StopAsync();
return exitCode;
