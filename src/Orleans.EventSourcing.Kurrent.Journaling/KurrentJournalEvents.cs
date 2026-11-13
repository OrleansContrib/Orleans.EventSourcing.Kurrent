using System.Buffers;
using System.Text;

using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Journaling;

/// <summary>
///     Encodes journal bytes as KurrentDB events and decodes them again.
/// </summary>
/// <remarks>
///     <para>
///     Orleans owns the journal's byte format; this provider stores those bytes opaquely. Each event's data is a
///     small header followed by journal bytes: <c>[version: 1 byte][format key length: 1 byte][format key: UTF-8]</c>.
///     The header is in the data rather than the event metadata because multi-stream appends re-encode event metadata.
///     </para>
///     <para>
///     The event type marks where a snapshot starts. Recovery reads backwards until it finds the first event of the
///     latest snapshot, so it never depends on the stream having been truncated.
///     </para>
/// </remarks>
internal static class KurrentJournalEvents
{
    /// <summary>Journal bytes appended after the latest snapshot.</summary>
    public const string AppendEventType = "orleans.journal.append";

    /// <summary>The first event of a snapshot; recovery starts here.</summary>
    public const string SnapshotEventType = "orleans.journal.snapshot";

    /// <summary>A later event of a snapshot that did not fit in one event.</summary>
    public const string SnapshotContinuedEventType = "orleans.journal.snapshot.continued";

    /// <summary>
    ///     Marks where the journal was deleted: it is empty from here. Deleting writes this before soft-deleting the stream,
    ///     so the stream's last revision moves past any revision another writer holds.
    /// </summary>
    public const string DeletedEventType = "orleans.journal.deleted";

    private const byte HeaderVersion = 1;
    private const string ContentType = "application/octet-stream";

    /// <summary>
    ///     Encodes the header written before every event's journal bytes.
    /// </summary>
    public static byte[] CreateHeader(string formatKey)
    {
        var formatKeyBytes = Encoding.UTF8.GetBytes(formatKey);
        if (formatKeyBytes.Length > byte.MaxValue)
        {
            throw new ArgumentException($"The journal format key must be at most {byte.MaxValue} bytes when UTF-8 encoded.", nameof(formatKey));
        }

        var header = new byte[2 + formatKeyBytes.Length];
        header[0] = HeaderVersion;
        header[1] = (byte)formatKeyBytes.Length;
        formatKeyBytes.CopyTo(header, 2);
        return header;
    }

    /// <summary>
    ///     Splits <paramref name="value"/> into events of at most <paramref name="maxEventSize"/> journal bytes each.
    /// </summary>
    /// <remarks>
    ///     A snapshot always produces at least one event, even when empty, so that it marks where recovery starts.
    /// </remarks>
    public static EventData[] Create(ReadOnlySequence<byte> value, byte[] header, bool isSnapshot, int maxEventSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxEventSize);

        var eventCount = (int)Math.Max(isSnapshot ? 1 : 0, (value.Length + maxEventSize - 1) / maxEventSize);
        var events = new EventData[eventCount];
        var remaining = value;
        for (var i = 0; i < eventCount; i++)
        {
            var chunk = remaining.Slice(0, Math.Min(remaining.Length, maxEventSize));
            remaining = remaining.Slice(chunk.End);

            var data = new byte[header.Length + chunk.Length];
            header.CopyTo(data, 0);
            chunk.CopyTo(data.AsSpan(header.Length));

            var eventType = !isSnapshot ? AppendEventType
                : i == 0 ? SnapshotEventType
                : SnapshotContinuedEventType;

            events[i] = new EventData(Uuid.NewUuid(), eventType, data, contentType: ContentType);
        }

        return events;
    }

    /// <summary>
    ///     Creates the event that marks where the journal was deleted. It carries the header and no journal bytes.
    /// </summary>
    public static EventData CreateDeleted(byte[] header)
        => new(Uuid.NewUuid(), DeletedEventType, header.ToArray(), contentType: ContentType);

    /// <summary>
    ///     Decodes an event written by <see cref="Create"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">The event was not written by this provider.</exception>
    public static JournalEvent Parse(EventRecord eventRecord)
    {
        var kind = eventRecord.EventType switch
        {
            AppendEventType => JournalEventKind.Append,
            SnapshotEventType => JournalEventKind.Snapshot,
            SnapshotContinuedEventType => JournalEventKind.SnapshotContinued,
            DeletedEventType => JournalEventKind.Deleted,
            _ => throw new InvalidOperationException(
                $"Event {eventRecord.EventNumber} in journal stream '{eventRecord.EventStreamId}' has unexpected type '{eventRecord.EventType}'."),
        };

        var data = eventRecord.Data;
        var span = data.Span;
        if (span.Length < 2 || span[0] != HeaderVersion || span.Length < 2 + span[1])
        {
            throw new InvalidOperationException(
                $"Event {eventRecord.EventNumber} in journal stream '{eventRecord.EventStreamId}' has an unsupported or corrupt header.");
        }

        var formatKey = Encoding.UTF8.GetString(span.Slice(2, span[1]));
        return new JournalEvent(kind, formatKey, data[(2 + span[1])..]);
    }
}

internal enum JournalEventKind
{
    Append,
    Snapshot,
    SnapshotContinued,
    Deleted,
}

internal readonly record struct JournalEvent(JournalEventKind Kind, string FormatKey, ReadOnlyMemory<byte> Payload);
