using Orleans.Runtime;

namespace Orleans.EventSourcing.Kurrent.Journaling;

/// <summary>
///     Validates <see cref="KurrentJournalStorageOptions"/> at silo startup.
/// </summary>
internal sealed class KurrentJournalStorageOptionsValidator(KurrentJournalStorageOptions options, string name) : IConfigurationValidator
{
    public void ValidateConfiguration()
    {
        if (options.ClientSettings is null)
        {
            throw new OrleansConfigurationException($"Invalid {nameof(KurrentJournalStorageOptions)} for journal storage provider '{name}': {nameof(options.ClientSettings)} is required.");
        }

        if (options.GetStreamName is null)
        {
            throw new OrleansConfigurationException($"Invalid {nameof(KurrentJournalStorageOptions)} for journal storage provider '{name}': {nameof(options.GetStreamName)} is required.");
        }

        ThrowIfNotPositive(options.MaxEventSize, nameof(options.MaxEventSize));
        ThrowIfNotPositive(options.CompactionEventCountThreshold, nameof(options.CompactionEventCountThreshold));
        ThrowIfNotPositive(options.CompactionSizeThreshold, nameof(options.CompactionSizeThreshold));
        ThrowIfNotPositive(options.ReadPageSize, nameof(options.ReadPageSize));

        if (options.MaxMetadataConflictRetries < 0)
        {
            throw new OrleansConfigurationException($"Invalid {nameof(KurrentJournalStorageOptions)} for journal storage provider '{name}': {nameof(options.MaxMetadataConflictRetries)} must not be negative.");
        }

        if (!Enum.IsDefined(options.AtomicCompaction))
        {
            throw new OrleansConfigurationException($"Invalid {nameof(KurrentJournalStorageOptions)} for journal storage provider '{name}': {nameof(options.AtomicCompaction)} has unknown value '{options.AtomicCompaction}'.");
        }
    }

    private void ThrowIfNotPositive(long value, string propertyName)
    {
        if (value <= 0)
        {
            throw new OrleansConfigurationException($"Invalid {nameof(KurrentJournalStorageOptions)} for journal storage provider '{name}': {propertyName} must be positive.");
        }
    }
}
