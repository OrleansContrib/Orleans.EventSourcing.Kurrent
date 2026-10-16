# Orleans.EventSourcing.Kurrent

[![NuGet](https://img.shields.io/nuget/v/Orleans.EventSourcing.Kurrent.svg)](https://www.nuget.org/packages/Orleans.EventSourcing.Kurrent)

[Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/) providers backed by [KurrentDB](https://kurrent.io/) (formerly EventStoreDB)

This project adds KurrentDB support to Microsoft Orleans.

| Provider | Orleans interface | What it persists | When to use |
| --- | --- | --- | --- |
| **Cluster membership provider** (`UseKurrentClustering`) | `IMembershipTable` `IGatewayListProvider` | Cluster membership information | Use this to maintain cluster membership in KurrentDB. | 
| **Log-consistency provider** (`AddKurrentBasedLogConsistencyProvider*`) | `ILogViewAdaptorFactory` for `JournaledGrain<TView, TEntry>` | The full **event log** for the grain — every `RaiseEvent` is appended to the grain's Kurrent stream. The view is rebuilt by replaying events. | Event-sourced grains (`JournaledGrain`). Gives you full history, projections via `IGrainEventProvider`, and replay. |
| **Grain state storage provider** (`AddKurrentBasedGrainStorageProvider*`) | `IGrainStorage` for `Grain<TState>` / `[PersistentState]` | The **latest snapshot** only — each `WriteStateAsync` appends one event and Kurrent's `MaxCount = 1` stream metadata trims older revisions. | Regular Orleans state persistence when you want it backed by Kurrent (e.g. to keep all storage in one system) |
| **Reminders provider** (`AddKurrentReminderService`) ⚠ experimental | `IReminderTable` | Grain Reminders | Use this provider to persist and manage Orleans reminders in KurrentDB. Installed from a separate package `Orleans.EventSourcing.Kurrent.Reminders`. |
| **Journaling storage provider** (`AddKurrentJournalStorage*`) ⚠ experimental | `IJournalStorageProvider` for Orleans Journaling (`IDurableStateManager`, `DurableGrain`) | Each grain's durable state journal, compacted into snapshots with older events truncated. | Durable collections and values with Orleans Journaling. Installed from a separate prerelease package `Orleans.EventSourcing.Kurrent.Journaling`. |
| **Grain event subscription** (`IGrainEventProvider`) ⚠ experimental | n/a | Read-side subscription to the Kurrent `$all` stream, filtered to the streams produced by the log-consistency provider. | Projections / read models built from grain events emitted via the log-consistency provider. |

## Install

```sh
dotnet add package Orleans.EventSourcing.Kurrent
```

The reminders provider is shipped as a separate package:

```sh
dotnet add package Orleans.EventSourcing.Kurrent.Reminders
```

The Orleans Journaling storage provider is shipped as a separate prerelease package, because Orleans Journaling itself is only published as a prerelease (it requires Orleans 10.4):

```sh
dotnet add package Orleans.EventSourcing.Kurrent.Journaling --prerelease
```

Targets `net8.0` and `net10.0`. Requires Microsoft Orleans 10.x and a KurrentDB / EventStoreDB server.

## Quick start

### Silo configuration

Pick whichever providers you need — they are configured separately and can use different options names if you want different `ClientSettings` per provider:

```csharp
using KurrentDB.Client;
using Orleans.EventSourcing.Kurrent.Hosting;
using Orleans.EventSourcing.Kurrent.Reminders;

var settings = KurrentDBClientSettings.Create("esdb://localhost:2113?tls=false");

var builder = Host.CreateApplicationBuilder(args);
builder.UseOrleans(silo =>
{
    // Clustering provider - supports the Orleans membership table implemented via events
    silo.UseKurrentClustering(o => o.ClientSettings = settings);

    // Log-consistency provider — required for JournaledGrain<TView, TEntry>.
    silo.AddKurrentBasedLogConsistencyProviderAsDefault(o => o.ClientSettings = settings);

    // Grain state storage provider — required for Grain<TState> / [PersistentState] backed by Kurrent.
    silo.AddKurrentBasedGrainStorageProviderAsDefault(o => o.ClientSettings = settings);

    // Reminders provider — if you want to persist Orleans reminders in Kurrent.
    silo.AddKurrentReminderService(o => o.ClientSettings = settings);
});
```

Configure the Orleans client to use the Kurrent Clustering provider:

```csharp
using KurrentDB.Client;
using Orleans.EventSourcing.Kurrent.Hosting;

var settings = KurrentDBClientSettings.Create("esdb://localhost:2113?tls=false");

var builder = Host.CreateApplicationBuilder(args);
builder.UseOrleansClient(client =>
{
    // Clustering provider - supports discovering silos by reading the cluster's event stream
    client.UseKurrentClustering(o => o.ClientSettings = settings);
});
```

The storage extensions also have non-default overloads that take a provider `name` so you can register multiple instances side-by-side.

To run KurrentDB locally (running **insecure** for testing only):
```sh
docker run --name kurrentdb-node -it -p 2113:2113 docker.kurrent.io/kurrent-latest/kurrentdb:latest --insecure
```

## Using the clustering provider

The clustering provider event-sources the orleans `IMembershipTable` and by writing membership events to the cluster's event stream.

The stream contains `MembershipUpdate` events written by silos as they join or when their status or suspect list changes. Silos also update their `IAmAlive` value every 30 seconds by default; these are recorded as `SiloAlive` events.

In addition to removing defunct silos from the membership table, the interval configured for [ClusteringMembershipOptions.DefunctSiloCleanupPeriod](https://learn.microsoft.com/en-us/dotnet/api/orleans.configuration.clustermembershipoptions.defunctsilocleanupperiod?view=orleans-10.0#orleans-configuration-clustermembershipoptions-defunctsilocleanupperiod)
may be used to write a snapshot of the membership table and truncate the cluster's event stream. This prevents the stream length growing indefinitely and ensures recovery and discovery complete in a reasonable amount of time.

### Configuring the clustering provider

The clustering provider can be configured using the delegate supplied to `UseKurrentClustering` 

Options available in `KurrentClusteringOptions` include:

| Name | Type | Default | Description |
|---|---|---|---|
| ClientSettings | KurrentDBClientSettings | null | The KurrentDB client settings used to connect to the KurrentDB server. |
| StreamPrefix | string | "Orleans.Cluster.Membership" | The prefix for the stream name used by the clustering provider. |
| EventCountBeforeSnapshot | int | 5000 | The number of events to write before taking a snapshot of the membership table. |

## Using the log-consistency provider (event-sourced grains)

Inherit from `JournaledGrain<TView, TEntry>`. Events are appended to a Kurrent stream per grain, and the view is rebuilt by replaying them.

> **Note:** Grains replay all events from the stream on activation. For long-running grains, use `[DiscardPriorEvents]` on summary or snapshot events to bound replay cost.

```csharp
public sealed class AccountGrain : JournaledGrain<AccountState, AccountEvent>, IAccountGrain
{
    public Task Deposit(decimal amount)
    {
        RaiseEvent(new AccountEvent.Deposited(amount));
        return ConfirmEvents();
    }
}
```

### Handling concurrent writes (`RaiseConditionalEvent`)

Every append uses optimistic concurrency: the provider writes with the exact stream revision it last observed, so a write is rejected if the stream was mutated by another writer (for example a duplicate activation during cluster membership changes, or an external process writing to the grain's stream).

- `RaiseEvent` + `ConfirmEvents()`: a rejected write surfaces as an `InconsistentStateException`, which causes Orleans to deactivate the grain. The next activation replays the stream and sees the true state.
- `RaiseConditionalEvent`: a rejected write returns `false` and the event is discarded — it is **never** retried automatically, because the conditions under which it was raised may no longer hold.

After `RaiseConditionalEvent` returns `false`, the in-memory views are stale: the tentative view still reflects the rejected event, and the confirmed view is missing the competing writer's events. You must resynchronize (or deactivate) before trusting the state or writing again. The correct retry pattern is:

```csharp
public async Task<bool> Withdraw(decimal amount, CancellationToken token)
{
    while (true)
    {
        token.ThrowIfCancellationRequested();
        
        // Re-validate against the current state on every iteration.
        if (State.Balance < amount)
        {
            return false;
        }

        if (await RaiseConditionalEvent(new AccountEvent.Withdrawn(amount)))
        {
            return true;
        }

        // The write was rejected: resynchronize with the stream before re-deciding.
        await RefreshNow();
    }
}
```

Consider bounding the loop (retry cap or backoff): persistent rejections usually indicate a competing writer, and throwing after a few attempts lets the activation recycle and recover cleanly.

### Truncating the event stream - Experimental

The provider supports truncating the event stream by using the `DiscardPriorEvents` attribute on an event type (`OEK0002` — suppress with `#pragma warning disable OEK0002` or `<NoWarn>$(NoWarn);OEK0002</NoWarn>`).

When an event with this attribute is committed, all prior events in the stream will be discarded.

This can be used to write a tombstone event e.g. `AccountClosed` or a summary event e.g. `AccountBalance`:

```csharp
[DiscardPriorEvents]
[Alias("Account.Closed.V1")]
public record AccountClosed(DateTime ClosedAt) : AccountEvent;

public sealed class AccountGrain : JournaledGrain<AccountState, AccountEvent>, IAccountGrain
{
    public Task CloseAccount()
    {
        RaiseEvent(new AccountClosed(DateTime.UtcNow));
        return ConfirmEvents();
    }
}
```

### Deleting the event stream

The log-consistency provider [tombstones](https://docs.kurrent.io/clients/python/v1.2/delete-stream.html#tombstone-stream) the event stream using `JournaledGrain.ClearLogAsync`.

> **Note:** Once tombstoned a stream cannot be reused in Kurrent. To truncate the stream use the `DiscardPriorEvents` attribute instead.

The state-based provider [soft deletes](https://docs.kurrent.io/clients/python/v1.2/delete-stream.html#delete-stream) the stream using `Grain<TState>.ClearStateAsync`, which will delete the stream and allow it to be reused in Kurrent.

### Subscribing to grain events (projections) - Experimental

Resolve `IGrainEventProvider` from the silo container (`OEK0001` — suppress with `#pragma warning disable OEK0001` or `<NoWarn>$(NoWarn);OEK0001</NoWarn>`) and call one of the `SubscribeToGrainEvents*` overloads. This reads the Kurrent `$all` stream filtered to streams written by the log-consistency provider, deserializes each event, and yields it together with the originating `GrainId` and version:

```csharp
await foreach (var update in eventProvider.SubscribeToGrainEvents<IAccountGrain, AccountEvent>(
    subscriber: this.GetGrainId(),
    startingPosition: GlobalEventLogPosition.Start,
    cancellationToken: ct))
{
    switch (update)
    {
        case GrainEvent<AccountEvent> e:
            // project e.Event for e.EventGrainId at e.EventGrainVersion
            break;
        case Checkpoint cp:
            // persist cp.Position so the next subscription resumes from here
            break;
        case CaughtUp:
            // subscription is now live
            break;
        case FallenBehind:
            // subscription is lagging behind the live tail
            break;
    }
}
```

`IGrainEventProvider` only sees streams produced by the **log-consistency** provider. The state-storage provider's snapshot writes are not surfaced through this API.

## Using the grain state storage provider

Use as a regular Orleans `IGrainStorage` — for example via `[PersistentState]`. Each `WriteStateAsync` appends one event to the grain's stream and Kurrent retains only the latest revision (stream metadata `MaxCount = 1`):

```csharp
public sealed class SettingsGrain(
    [PersistentState(stateName: "settings")] IPersistentState<Settings> state)
    : Grain, ISettingsGrain
{
    public Task<Settings> Get() => Task.FromResult(state.State);

    public Task Set(Settings value)
    {
        state.State = value;
        return state.WriteStateAsync();
    }
}
```

## Reminders provider

The reminders provider is shipped as a **separate package**:

```sh
dotnet add package Orleans.EventSourcing.Kurrent.Reminders
```

It records Orleans reminder upserts and removals as discrete events in KurrentDB via a `JournaledGrain` implementing `IReminderTable`.

Register it on the silo:

```csharp
using Orleans.EventSourcing.Kurrent.Reminders;

builder.UseOrleans(silo =>
{
    silo.AddKurrentReminderService(o => o.ClientSettings = settings);
});
```

Reminders are persisted to a single stream. If you have high reminder churn, the stream will become large and it will take time to read the entire stream on startup.

## Journaling storage provider - Experimental

[Orleans Journaling](https://github.com/dotnet/orleans/tree/main/src/Orleans.Journaling) persists durable grain state (`IDurableStateManager`, `DurableGrain`, `IDurableDictionary`, `IDurableList`, `IDurableValue` and friends) as a journal of operations plus periodic snapshots. This provider stores each journal in its own KurrentDB stream. It ships as a **separate package** because Orleans Journaling is only published as a prerelease and requires Orleans 10.4:

```sh
dotnet add package Orleans.EventSourcing.Kurrent.Journaling --prerelease
```

Register it on the silo, together with the JSON metadata for your journaled types:

```csharp
using System.Text.Json.Serialization;
using Orleans.EventSourcing.Kurrent.Journaling;
using Orleans.Journaling.Json;

builder.UseOrleans(silo =>
{
    silo.AddKurrentJournalStorageAsDefault(o => o.ClientSettings = settings)
        .UseJsonJournalFormat(JournalJsonContext.Default);
});

// JSON journaling needs source-generated metadata for every journaled key, value and state type,
// plus string, uint and DateTime, which Orleans uses to track state names and retirements.
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(uint))]
[JsonSerializable(typeof(DateTime))]
internal partial class JournalJsonContext : JsonSerializerContext;
```

Orleans Journaling APIs raise the `ORLEANSEXP005` diagnostic; suppress it with `<NoWarn>$(NoWarn);ORLEANSEXP005</NoWarn>`.

### How journals are stored

- Each journal is stored in the stream `orleansjournal-{JournalId}` (override with `KurrentJournalStorageOptions.GetStreamName`). The prefix makes `orleansjournal` the KurrentDB category of every journal stream.
- Each append becomes an `orleans.journal.append` event. Writes larger than `MaxEventSize` (512 KiB) are split across several events in one atomic append.
- Concurrent writers are detected with the stream revision: a second activation writing the same journal fails with `InconsistentStateException`.
- Once `CompactionEventCountThreshold` (1,000) events or `CompactionSizeThreshold` (4 MiB) of journal bytes have been appended since the last snapshot, Orleans asks for compaction. The provider then appends an `orleans.journal.snapshot` event and moves the stream's `$tb` (truncate before) metadata to it, so older events are hidden and later scavenged. Other stream metadata (ACLs, `$maxAge`, custom properties) is preserved.
- Recovery reads backwards to the latest snapshot and replays from there, so it never depends on truncation having succeeded.
- Clearing state soft-deletes the stream, so the journal can be written again later.

### Atomic compaction

On KurrentDB 25.1 and later, compaction writes the snapshot and the `$tb` metadata in a single atomic multi-stream append to the journal stream and its metastream (`$$orleansjournal-…`). On older servers it appends the snapshot and then updates the metadata as a separate, best-effort step; if that second write fails, the older events simply stay until the next compaction. The provider checks the server's capabilities each time it compacts, so it notices upgrades and failovers. `KurrentJournalStorageOptions.AtomicCompaction` controls this:

| Value | Behaviour |
| --- | --- |
| `Auto` (default) | Atomic when the server supports multi-stream appends, otherwise two writes. |
| `Required` | Always atomic; compaction fails on servers older than 25.1. |
| `Disabled` | Always two writes. |

The account the provider connects with needs write access to the journal streams' metastreams, as it does for any stream metadata change.

### Testing against a real server

The journaling tests use the in-memory emulator by default. To run them against a real server, set `KURRENT_JOURNALING_TESTS_CONNECTION_STRING`, for example:

```bash
docker run -d -p 2113:2113 -e KURRENTDB_INSECURE=true -e KURRENTDB_MAX_APPEND_SIZE=1048576 -e KURRENTDB_MAX_APPEND_EVENT_SIZE=1048576 docker.kurrent.io/kurrent-latest/kurrentdb:25.1
KURRENT_JOURNALING_TESTS_CONNECTION_STRING="kurrentdb://localhost:2113?tls=false" dotnet test tests/Orleans.EventSourcing.Kurrent.Journaling.Tests
```

A server's support for multi-stream appends depends on its version, so tests that need a server with (25.1+) or without it are skipped on the other kind. The `MaxAppendSize` test is skipped when the server's limit is above 16 MiB, as it is by default from 25.1; the command above lowers it to 1 MiB.

### Limitations

- Every write must fit within the server's `MaxAppendSize`, including whole snapshots. It defaults to 1 MiB before KurrentDB 25.1 and to 256 MiB from 25.1, which also limits each event to `MaxAppendEventSize` (16 MiB; keep `MaxEventSize` below it). Raise `MaxAppendSize` on the server if your durable state is larger. An append over the limit fails with `MaximumAppendSizeExceededException`. An atomic compaction over the limit fails with an `RpcException` (`ResourceExhausted`). On .NET 8 that compaction can stall instead, and it then fails with `DeadlineExceeded` after the client's `DefaultDeadline` (10 seconds by default).
- KurrentDB.Client 1.2.0 sends multi-stream appends without a deadline and replaces their transport errors with a `NullReferenceException`. The provider applies `ClientSettings.DefaultDeadline` to them and rethrows the original `RpcException`.
- In Orleans 10.4, grain durable state always uses the default journal storage provider. Named providers (`AddKurrentJournalStorage("name", ...)`) are reachable through the keyed `IJournaledStateManagerFactory`.
- Journal metadata operations (`GetMetadataAsync`, `UpdateMetadataAsync`, `CreateIfNotExistsAsync`) and journal catalog listing (`IJournalStorageCatalog`) are not implemented yet. Orleans' state manager does not use them.

## Configuration

`KurrentStorageOptions` is configured per provider name. The same options type is used by both storage providers; configuring one named instance does not affect the other.

| Property | Description |
| --- | --- |
| `ClientSettings` | `KurrentDBClientSettings` used to build the underlying `KurrentClient`. |
| `GrainStorageSerializer` | Orleans' `IGrainStorageSerializer` used by the default event serializer. |
| `StreamNameProvider` | `IKurrentStreamNameProvider` controlling stream-name layout. Defaults to `KurrentStreamName.Default`. |

### Custom event stream naming

The naming scheme is pluggable — see [Custom event stream naming](#custom-event-stream-naming).

Both providers, and the projection subscription, route through `IKurrentStreamNameProvider`. The default produces:

- `{GrainType}-{Key}` for log-consistency streams.
- `{GrainType}-{stateName}|{Key}` for state-storage streams.

To change the scheme, implement `IKurrentStreamNameProvider` and assign it on the options for each provider you register:

```csharp
silo.AddKurrentBasedLogConsistencyProviderAsDefault(o =>
{
    o.ClientSettings = settings;
    o.StreamNameProvider = new MyStreamNameProvider();
});

silo.AddKurrentBasedGrainStorageProviderAsDefault(o =>
{
    o.ClientSettings = settings;
    o.StreamNameProvider = new MyStreamNameProvider();
});
```

### Event type names and `[Alias]`

The default serializers (`DefaultEventSerializer<T>` and `EventEnvelopeSerializer<T>`) write each event's CLR type into Kurrent's `EventType` field via Orleans' `TypeConverter.Format(Type)`. That converter honours `[Alias("...")]` from `Orleans.Metadata`:

- If the event type carries `[Alias("MyAlias")]`, the Kurrent `EventType` is `MyAlias`.
- Otherwise, the converter falls back to the Orleans assembly-qualified name format.

Implications:

- **`[Alias]` is the stable contract** between your app and Kurrent. Renaming or moving an event type without an `[Alias]` will break replay because previously written `EventType` strings will no longer resolve. Always add `[Alias]` to event records you persist.
- **Aliases must be unique** across the silo's serialization graph; this is an Orleans-wide constraint, not specific to this package.

```csharp
[GenerateSerializer]
[Alias("Account.Deposited")]
public sealed record Deposited(decimal Amount);
```

If you need a different encoding (e.g., a versioned namespace scheme, JSON envelope with a discriminator, or interop with a non-Orleans producer), implement `IEventSerializer<TLogEntry>` and register it via DI before the provider, or supply your own through `KurrentStorageOptions`.

## Accessing EventId and Event Metadata

If you need access to Kurrent's `EventId` or metadata fields in your grain, wrap your `TLogEvent` using `EventEnvelope<TLogEvent>` as your TLogEvent argument for JournaledGrain or IGrainEventProvider.

e.g. `IGrainEventProvider.SubscribeToGrainEvents<EventEnvelope<T>>` and `JournaledGrain<TLogView, EventEnvelope<TLogEvent>>`.

You can mix-and-match `EventEnvelope<T>` with `T` as needed within the same silo, as the serializer will handle both cases.

When you use `EventEnvelope<T>` a different `IEventConverter` is used which reads and writes the properties of the `EventEnvelope` into the appropriate Kurrent fields.

* EventId property, which you can set if you want control over the eventId written to KurrentDB, or read from the event when replaying.
* Metadata dictionary, which is persisted to KurrentDB and can be used to store additional information about the event that doesn't fit into the event payload. This can be useful for things like correlation ids, causation ids, or any other contextual information you want to associate with the event.

## Notes & limitations

- Read-optimisation snapshots are not yet supported; an event-sourced grain will replay all events on activation. Business snapshots are supported using events marked with `[DiscardPriorEvents]`.

## Versioning

Package versions are derived from git tags (`vMAJOR.MINOR.PATCH[-prerelease]`) using [MinVer](https://github.com/adamralph/minver). Tagged commits publish stable versions; non-tag commits produce pre-release packages.

## Experimental APIs

Some surface is annotated with `[Experimental("OEK…")]` and will produce a compiler diagnostic at every call site. The shape of these APIs may change in non-major releases. Acknowledge by suppressing the relevant diagnostic ID in your project (`<NoWarn>$(NoWarn);OEK0001;OEK0002</NoWarn>`) or with a localized `#pragma warning disable`.

| Diagnostic | API | Purpose |
| --- | --- | --- |
| `OEK0001` | `IGrainEventProvider` | An interface available via dependency injection for grains hosting projections or side-effects to read grain events |
| `OEK0002` | `DiscardPriorEventsAttribute`  | An attribute to tell the storage provider to delete events prior to this event |

## License

[License](https://github.com/OrleansContrib/Orleans.EventSourcing.Kurrent/blob/main/LICENSE.md)

