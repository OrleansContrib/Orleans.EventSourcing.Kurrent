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
        if (_options.Retry is null)
        {
            throw new OrleansConfigurationException($"Invalid configuration for {nameof(KurrentStorageOptions)} with name {name}. {nameof(KurrentStorageOptions)}.{nameof(_options.Retry)} is required.");
        }
        if (_options.Retry.MaxAttempts < 1)
        {
            throw new OrleansConfigurationException($"Invalid configuration for {nameof(KurrentStorageOptions)} with name {name}. {nameof(KurrentStorageOptions)}.{nameof(_options.Retry)}.{nameof(KurrentRetryOptions.MaxAttempts)} must be at least 1.");
        }
        if (_options.Retry.BaseDelay < TimeSpan.Zero || _options.Retry.MaxDelay < _options.Retry.BaseDelay)
        {
            throw new OrleansConfigurationException($"Invalid configuration for {nameof(KurrentStorageOptions)} with name {name}. {nameof(KurrentStorageOptions)}.{nameof(_options.Retry)} requires 0 <= {nameof(KurrentRetryOptions.BaseDelay)} <= {nameof(KurrentRetryOptions.MaxDelay)}.");
        }
        if (_options.Retry.MaxDuration <= TimeSpan.Zero)
        {
            throw new OrleansConfigurationException($"Invalid configuration for {nameof(KurrentStorageOptions)} with name {name}. {nameof(KurrentStorageOptions)}.{nameof(_options.Retry)}.{nameof(KurrentRetryOptions.MaxDuration)} must be positive when set.");
        }
    }
}
