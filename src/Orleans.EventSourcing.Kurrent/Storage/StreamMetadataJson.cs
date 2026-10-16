using System.Text.Json;

using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Converts <see cref="StreamMetadata"/> to and from the JSON stored in a stream's metastream (<c>$$stream</c>).
/// </summary>
/// <remarks>
///     The client's own converter is internal; this mirrors its wire format so stream metadata can be written
///     as a <c>$metadata</c> event inside a multi-stream append, atomically with events in the stream itself.
/// </remarks>
internal static class StreamMetadataJson
{
    public const string MetadataEventType = "$metadata";

    const string MaxAge = "$maxAge";
    const string MaxCount = "$maxCount";
    const string TruncateBefore = "$tb";
    const string CacheControl = "$cacheControl";
    const string Acl = "$acl";
    const string AclRead = "$r";
    const string AclWrite = "$w";
    const string AclDelete = "$d";
    const string AclMetaRead = "$mr";
    const string AclMetaWrite = "$mw";

    /// <summary>
    ///     Returns the name of the metastream holding <paramref name="streamName"/>'s metadata.
    /// </summary>
    public static string GetMetastreamName(string streamName) => $"$${streamName}";

    /// <summary>
    ///     Returns <see langword="true"/> and the owning stream if <paramref name="streamName"/> is a metastream.
    /// </summary>
    public static bool TryGetOwningStream(string streamName, out string owningStream)
    {
        if (streamName.Length > 2 && streamName.StartsWith("$$", StringComparison.Ordinal))
        {
            owningStream = streamName[2..];
            return true;
        }

        owningStream = string.Empty;
        return false;
    }

    /// <summary>
    ///     Creates the <c>$metadata</c> event which sets <paramref name="metadata"/> when appended to a metastream.
    /// </summary>
    public static EventData CreateMetadataEvent(StreamMetadata metadata)
        => new(Uuid.NewUuid(), MetadataEventType, Serialize(metadata), contentType: "application/json");

    public static byte[] Serialize(StreamMetadata metadata)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();

            if (metadata.MaxCount.HasValue)
            {
                writer.WriteNumber(MaxCount, metadata.MaxCount.Value);
            }

            if (metadata.MaxAge.HasValue)
            {
                writer.WriteNumber(MaxAge, (long)metadata.MaxAge.Value.TotalSeconds);
            }

            if (metadata.TruncateBefore.HasValue)
            {
                writer.WriteNumber(TruncateBefore, metadata.TruncateBefore.Value.ToInt64());
            }

            if (metadata.CacheControl.HasValue)
            {
                writer.WriteNumber(CacheControl, (long)metadata.CacheControl.Value.TotalSeconds);
            }

            if (metadata.Acl is { } acl)
            {
                writer.WritePropertyName(Acl);
                writer.WriteStartObject();
                WriteRoles(writer, AclRead, acl.ReadRoles);
                WriteRoles(writer, AclWrite, acl.WriteRoles);
                WriteRoles(writer, AclDelete, acl.DeleteRoles);
                WriteRoles(writer, AclMetaRead, acl.MetaReadRoles);
                WriteRoles(writer, AclMetaWrite, acl.MetaWriteRoles);
                writer.WriteEndObject();
            }

            if (metadata.CustomMetadata is { RootElement.ValueKind: JsonValueKind.Object } customMetadata)
            {
                foreach (var property in customMetadata.RootElement.EnumerateObject())
                {
                    property.WriteTo(writer);
                }
            }

            writer.WriteEndObject();
        }

        return stream.ToArray();
    }

    public static StreamMetadata Deserialize(ReadOnlySpan<byte> json)
    {
        int? maxCount = null;
        TimeSpan? maxAge = null;
        TimeSpan? cacheControl = null;
        StreamPosition? truncateBefore = null;
        StreamAcl? acl = null;

        using var document = JsonDocument.Parse(json.ToArray());
        using var customStream = new MemoryStream();
        using (var customWriter = new Utf8JsonWriter(customStream))
        {
            customWriter.WriteStartObject();
            foreach (var property in document.RootElement.EnumerateObject())
            {
                switch (property.Name)
                {
                    case MaxCount:
                        maxCount = property.Value.GetInt32();
                        break;
                    case MaxAge:
                        maxAge = TimeSpan.FromSeconds(property.Value.GetInt64());
                        break;
                    case CacheControl:
                        cacheControl = TimeSpan.FromSeconds(property.Value.GetInt64());
                        break;
                    case TruncateBefore:
                        var value = property.Value.GetInt64();
                        truncateBefore = value == long.MaxValue ? StreamPosition.End : StreamPosition.FromInt64(value);
                        break;
                    case Acl:
                        acl = new StreamAcl(ReadRoles(property.Value, AclRead),
                                            ReadRoles(property.Value, AclWrite),
                                            ReadRoles(property.Value, AclDelete),
                                            ReadRoles(property.Value, AclMetaRead),
                                            ReadRoles(property.Value, AclMetaWrite));
                        break;
                    default:
                        property.WriteTo(customWriter);
                        break;
                }
            }
            customWriter.WriteEndObject();
        }

        return new StreamMetadata(maxCount, maxAge, truncateBefore, cacheControl, acl, JsonDocument.Parse(customStream.ToArray()));
    }

    /// <summary>
    ///     Returns a copy of <paramref name="metadata"/> with <see cref="StreamMetadata.TruncateBefore"/> replaced.
    /// </summary>
    public static StreamMetadata WithTruncateBefore(this StreamMetadata metadata, StreamPosition truncateBefore)
        => new(metadata.MaxCount, metadata.MaxAge, truncateBefore, metadata.CacheControl, metadata.Acl, metadata.CustomMetadata);

    private static void WriteRoles(Utf8JsonWriter writer, string name, string[]? roles)
    {
        if (roles is null)
        {
            return;
        }

        writer.WritePropertyName(name);
        writer.WriteStartArray();
        foreach (var role in roles)
        {
            writer.WriteStringValue(role);
        }
        writer.WriteEndArray();
    }

    private static string[]? ReadRoles(JsonElement acl, string name)
    {
        if (!acl.TryGetProperty(name, out var roles))
        {
            return null;
        }

        return roles.ValueKind switch
        {
            JsonValueKind.String => [roles.GetString()!],
            JsonValueKind.Array => [.. roles.EnumerateArray().Select(x => x.GetString()!)],
            _ => null,
        };
    }
}
