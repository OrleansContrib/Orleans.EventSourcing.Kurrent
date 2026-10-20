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
// adds --step-rate every --step-seconds, reporting throughput, latency and failures per step:
//   --start-rate <n>         initial target ops/sec
//   --step-rate <n>          ops/sec added each step
//   --step-seconds <n>       duration of each step
//   --max-steps <n>          number of steps                          (default 20)
//   --max-rate <n>           cap on the target rate
//   --max-in-flight <n>      client-side concurrency cap; excess ops are shed (default 10000)
//   --max-error-rate <f>     stop when a step's failure ratio exceeds this (default 0.01)
//   --max-p99-ms <n>         stop when a step's p99 latency exceeds this (default off)
//   --min-achieved <f>       stop when a step achieves less than this fraction of target (default 0.9)
//   --csv <path>             write per-step results to a CSV file
//
// events — event write throughput through the log-consistency provider:
//   --grains <n>             number of event-writer grains            (default 1000)
//   --batch <n>              events raised per append                 (default 1)
//   --payload <bytes>        payload size per event                   (default 64)
//   (ramp defaults: start 200/s, +200/s every 30s)
//
// reminders — Kurrent reminder table registration throughput, then tick delivery and lateness:
//   --period <seconds>       reminder period, minimum 60              (default 60)
//   --observe <seconds>      how long to observe ticks after the ramp (default 3 x period)
//   --no-cleanup             leave the reminders registered afterwards
//   (ramp defaults: start 20/s, +20/s every 30s, 10 steps, stop on any failure)
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

var builder = Host.CreateApplicationBuilder();
builder.Logging.AddFilter("Orleans", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
builder.UseOrleansClient(client => client.UseKurrentClustering(x => x.ClientSettings = KurrentDBClientSettings.Create(connectionString)));

using var host = builder.Build();
await host.StartAsync();
var clusterClient = host.Services.GetRequiredService<IClusterClient>();
Console.WriteLine("Connected.");

// Ctrl+C stops the ramp early but still runs verification and cleanup.
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

var exitCode = scenario switch
{
    "events" => await EventsScenario.RunAsync(clusterClient, options, cancellation.Token),
    "reminders" => await RemindersScenario.RunAsync(clusterClient, options, cancellation.Token),
    "mixed" => await MixedScenario.RunAsync(clusterClient, options),
    _ => throw new ArgumentException($"Unknown scenario '{scenario}'. Expected events, reminders or mixed."),
};

await host.StopAsync();
return exitCode;
