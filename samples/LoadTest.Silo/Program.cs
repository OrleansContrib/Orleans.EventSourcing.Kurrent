using KurrentDB.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Dashboard;
using Orleans.EventSourcing.Kurrent.Hosting;
using Orleans.EventSourcing.Kurrent.Reminders;
using Orleans.Storage;

// Connection string for KurrentDB. The special host "kurrentemulator" selects the in-process,
// in-memory Kurrent implementation so the load test runs without external infrastructure.
// Point KURRENT_CONNECTION_STRING at a real KurrentDB instance to measure end-to-end performance.
var connectionString = Environment.GetEnvironmentVariable("KURRENT_CONNECTION_STRING")
                       ?? "esdb://localhost:2113?tls=false";

// GRAIN_SERIALIZER selects the event payload serializer for the log-consistency provider:
//   "orleans" (default) — Orleans binary serializer
//   "stj"               — System.Text.Json
// Note: streams written with one serializer cannot be read with the other, so wipe the
// database (docker compose down -v) when switching.
var serializerName = (Environment.GetEnvironmentVariable("GRAIN_SERIALIZER") ?? "default").ToLowerInvariant();

var clientSettings = KurrentDBClientSettings.Create(connectionString);

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.AddFilter("Orleans", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft", LogLevel.Warning);

builder.UseOrleans(silo =>
{
    silo.ConfigureEndpoints(Random.Shared.Next(29170, 29998), Random.Shared.Next(29170, 29998));
    silo.AddDashboard();
    silo.UseKurrentClustering(o => { o.ClientSettings = clientSettings; o.EventCountBeforeSnapshot = 50; });
    //silo.UseLocalhostClustering();
    silo.AddKurrentBasedLogConsistencyProviderAsDefault(optionsBuilder => optionsBuilder.Configure<IServiceProvider>((o, sp) =>
    {
        o.ClientSettings = clientSettings;
        if (serializerName == "stj")
        {
            o.GrainStorageSerializer = ActivatorUtilities.CreateInstance<SystemTextJsonGrainStorageSerializer>(sp);
        }
    }));
    silo.AddKurrentReminderService(o => o.ClientSettings = clientSettings);
});

var host = builder.Build();

// When launched by the load test client (the rolling scenario), stdin is a control channel: a "stop"
// line shuts the silo down gracefully, and so does end-of-stream, so a silo never outlives the client.
if (Environment.GetEnvironmentVariable("LOADTEST_STDIN_CONTROL") == "1")
{
    var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
    _ = Task.Run(async () =>
    {
        while (await Console.In.ReadLineAsync() is { } line && line.Trim() != "stop")
        {
        }

        lifetime.StopApplication();
    });
}

Console.WriteLine($"LoadTest silo starting (Kurrent: {connectionString}, serializer: {serializerName})...");
await host.StartAsync();
var siloAddress = host.Services.GetRequiredService<ILocalSiloDetails>().SiloAddress;
Console.WriteLine($"LoadTest silo is running at {siloAddress}. Press Ctrl+C to shut down.");
await host.WaitForShutdownAsync();
Console.WriteLine($"LoadTest silo {siloAddress} stopped.");
