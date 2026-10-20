using KurrentDB.Client;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.EventSourcing.Kurrent.Clustering.Events;
using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Storage;
using System.Text.Json;

namespace Orleans.EventSourcing.Kurrent.Clustering;

// Owns all persistence concerns for membership: Kurrent stream I/O, JSON serialization, stream naming,
// and tracking the StreamState each MembershipView was read at (its read cursor and optimistic-concurrency token).
internal sealed class KurrentMembershipEventStorage(IKurrentClient client, IOptions<KurrentClusteringOptions> options, IOptions<ClusterOptions> clusterOptions, JsonSerializerOptions jsonSerializerOptions)
{
    private readonly string streamName = $"{options.Value.StreamPrefix}/{clusterOptions.Value.ServiceId}/{clusterOptions.Value.ClusterId}";

    internal MembershipView InitialView { get; } = new() { StreamCursor = StreamState.NoStream };

    private static StreamPosition GetReadStreamPosition(StreamState streamCursor)
        => streamCursor == StreamState.NoStream ? StreamPosition.Start : StreamPosition.FromInt64(streamCursor.ToInt64()).Next();
    
    internal async Task<MembershipView> RefreshState(MembershipView view, CancellationToken cancellationToken)
    {
        var readResult = await client.ReadStreamAsync(Direction.Forwards, streamName, GetReadStreamPosition(view.StreamCursor), long.MaxValue, false, cancellationToken).ConfigureAwait(false);

        await foreach (var message in readResult.ConfigureAwait(false))
        {
            EventBase? deserializedEvent = message.Event.EventType switch
            {
                nameof(MembershipTableSnapshot) => JsonSerializer.Deserialize<MembershipTableSnapshot>(message.Event.Data.Span, jsonSerializerOptions),
                nameof(SiloAlive) => JsonSerializer.Deserialize<SiloAlive>(message.Event.Data.Span, jsonSerializerOptions),
                nameof(SiloAdded) => JsonSerializer.Deserialize<SiloAdded>(message.Event.Data.Span, jsonSerializerOptions),
                nameof(SiloDefunct) => JsonSerializer.Deserialize<SiloDefunct>(message.Event.Data.Span, jsonSerializerOptions),
                nameof(SiloStateUpdated) => JsonSerializer.Deserialize<SiloStateUpdated>(message.Event.Data.Span, jsonSerializerOptions),
                _ => null,
            };

            if (deserializedEvent is null)
            {
                throw new InvalidOperationException($"Event type {message.Event.EventType} could not be deserialized");
            }

            view = deserializedEvent.Apply(view) with { StreamCursor = StreamState.StreamRevision(message.Event.EventNumber.ToUInt64()) };
        }

        if (readResult.LastStreamPosition.HasValue)
        {
            return view with { StreamCursor = StreamState.StreamRevision(readResult.LastStreamPosition.Value.ToUInt64()) };
        }
        else
        {
            return view;
        }
    }

    private async Task<MembershipView> WriteSnapshot(MembershipView from, EventBase[] newEvents, CancellationToken cancellationToken)
    {
        MembershipView updatedView = from;

        // Incorporate the new events into the updated view
        foreach (var evt in newEvents)
        {
            updatedView = evt.Apply(updatedView);
        }

        // Write a snapshot of the updated view to the stream, using the stream cursor for optimistic concurrency
        if (await client.ConditionalAppendToStreamAsync(streamName,
                                                        updatedView.StreamCursor,
                                                        [new EventData(Uuid.NewUuid(), nameof(MembershipTableSnapshot), JsonSerializer.SerializeToUtf8Bytes<MembershipTableSnapshot>(updatedView.GetSnapshot(), jsonSerializerOptions))],
                                                        cancellationToken)
                        .ConfigureAwait(false) is { Status: ConditionalWriteStatus.Succeeded } success)
        {

            // Write the truncation point for the snapshot to the stream, using the stream cursor for optimistic concurrency            
            var truncationPoint = StreamPosition.FromInt64(success.NextExpectedVersion);

            while(true)
            {
                var readMetadata = await client.GetStreamMetadata(streamName, cancellationToken)
                                               .ConfigureAwait(false);
  

                if (readMetadata.Metadata.TruncateBefore >= truncationPoint)
                {
                    // Skip if another snapshot has been written after ours, and the truncation point has advanced beyond our event
                    break;
                }

                // Conditional metadata write, if the metadata revision has changed likely means the truncation point has changed
                try
                {
                    var writeMetadata = await client.SetStreamMetadata(streamName,
                                                                       readMetadata.MetastreamRevision.HasValue ? StreamState.StreamRevision(readMetadata.MetastreamRevision.Value) : StreamState.NoStream,
                                                                       new StreamMetadata(maxCount: readMetadata.Metadata.MaxCount,
                                                                                          maxAge: readMetadata.Metadata.MaxAge,
                                                                                          truncateBefore: truncationPoint,
                                                                                          cacheControl: readMetadata.Metadata.CacheControl,
                                                                                          acl: readMetadata.Metadata.Acl,
                                                                                          customMetadata: readMetadata.Metadata.CustomMetadata),
                                                                       cancellationToken)
                                                    .ConfigureAwait(false);

                    if (writeMetadata is WrongExpectedVersionResult)
                    {
                        continue; // try again
                    }
                }
                catch(WrongExpectedVersionException)
                {
                    continue; // try again
                }

                break; // Success
            }
        
            return updatedView with { StreamCursor = success.NextExpectedStreamState };
        }

        return from;
    }

    internal async Task<MembershipView> Write(MembershipView from, IEnumerable<EventBase> newEvents, CancellationToken cancellationToken)
    {
        if (newEvents.ToArray() is not { Length: >0 } writeEvents)
        {
            return from;
        }

        if (from.EventsSinceSnapshot + writeEvents.Length >= options.Value.EventCountBeforeSnapshot) // Have we reached the threshold for writing a full snapshot?
        {
            return await WriteSnapshot(from, writeEvents, cancellationToken)
                        .ConfigureAwait(false);
        }
      
        var expectedStreamState = writeEvents.All(x => x is SiloDefunct || x is SiloAlive) // These two events are dirty write safe
                                                       ? StreamState.Any 
                                                       : from.StreamCursor; // All other events require optimistic concurrency

        if (await client.ConditionalAppendToStreamAsync(streamName,
                                                        expectedStreamState,
                                                        writeEvents.Select(x=> new EventData(Uuid.NewUuid(), x.GetType().Name, JsonSerializer.SerializeToUtf8Bytes(x, x.GetType(), jsonSerializerOptions))),
                                                        cancellationToken)
                        .ConfigureAwait(false) is { Status: ConditionalWriteStatus.Succeeded } success)
        {
            foreach (var evt in writeEvents)
            {
                if (expectedStreamState == StreamState.Any)
                {
                    from = evt.Apply(from); // Do not update the stream cursor as we may not have observed intermediate events because this write was unconditional
                }
                else
                {
                    from = evt.Apply(from) with { StreamCursor = success.NextExpectedStreamState };
                }
            }
        }       

        return from;
    }

    internal async Task<MembershipView> Delete(MembershipView view, string clusterId, CancellationToken cancellationToken)
    {
        if (clusterOptions.Value.ClusterId != clusterId)
        { 
            return view;
        }

        _ = await client.DeleteStreamAsync(streamName, StreamState.Any, cancellationToken)
                        .ConfigureAwait(false);

        return InitialView;
    }
}