namespace Orleans.EventSourcing.Kurrent.Configuration;

internal sealed class KurrentStorageOptionsValidator(KurrentStorageOptions options, string name) : IConfigurationValidator
{
    private readonly KurrentStorageOptions _options = options ?? throw new OrleansConfigurationException($"Invalid KurrentStorageOptions for KurrentLogConsistentStorage {name}. Options is required.");

    /// <inheritdoc />
    public void ValidateConfiguration()
    {
        if (_options.ClientSettings is null)
        {
            throw new OrleansConfigurationException($"Invalid configuration for {nameof(KurrentStorageOptions)} with name {name}. {nameof(KurrentStorageOptions)}.{nameof(_options.ClientSettings)} is required.");
        }
        if (_options.StreamNameProvider is null)
        {
            throw new OrleansConfigurationException($"Invalid configuration for {nameof(KurrentStorageOptions)} with name {name}. {nameof(KurrentStorageOptions)}.{nameof(_options.StreamNameProvider)} is required.");
        }
        if (_options.Retry is null || _options.Retry.MaxAttempts < 1 || _options.Retry.BaseDelay < TimeSpan.Zero || _options.Retry.MaxDelay < _options.Retry.BaseDelay)
        {
            throw new OrleansConfigurationException($"Invalid configuration for {nameof(KurrentStorageOptions)} with name {name}. {nameof(KurrentStorageOptions)}.{nameof(_options.Retry)} requires MaxAttempts >= 1 and 0 <= BaseDelay <= MaxDelay.");
        }
    }
}
