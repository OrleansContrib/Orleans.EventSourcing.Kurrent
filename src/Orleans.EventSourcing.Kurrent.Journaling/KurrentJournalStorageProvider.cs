using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.Journaling;
using Orleans.Journaling.Json;

namespace Orleans.EventSourcing.Kurrent.Journaling;

/// <summary>
///     Creates <see cref="KurrentJournalStorage"/> instances which store each journal in its own KurrentDB stream.
/// </summary>
internal sealed class KurrentJournalStorageProvider : IJournalStorageProvider, IDisposable, IAsyncDisposable
{
    private readonly KurrentJournalStorageOptions options;
    private readonly IKurrentClient client;
    private readonly string journalFormatKey;
    private readonly ILogger logger;

    public KurrentJournalStorageProvider(KurrentJournalStorageOptions options,
                                         IKurrentClient client,
                                         IOptions<JournaledStateManagerOptions> journalOptions,
                                         ILogger<KurrentJournalStorageProvider> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(journalOptions);
        ArgumentNullException.ThrowIfNull(logger);

        this.options = options;
        this.client = client;
        this.logger = logger;

        // Every write is stamped with the configured format key so recovery can select the matching journal format.
        journalFormatKey = journalOptions.Value.JournalFormatKey is { Length: > 0 } formatKey
            ? formatKey
            : JsonLinesJournalFormat.JournalFormatKey;
    }

    /// <inheritdoc/>
    public IJournalStorage CreateStorage(JournalId journalId)
    {
        if (journalId.IsDefault)
        {
            throw new ArgumentException("The journal id must not be the default value.", nameof(journalId));
        }

        return new KurrentJournalStorage(options.GetStreamName(journalId), journalFormatKey, client, options, logger);
    }

    public void Dispose() => client.Dispose();

    public ValueTask DisposeAsync() => client.DisposeAsync();
}
