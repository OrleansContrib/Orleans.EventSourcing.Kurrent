using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Orleans.Configuration;
using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.Hosting;
using Orleans.Journaling;
using Orleans.Providers;
using Orleans.Runtime;

namespace Orleans.EventSourcing.Kurrent.Journaling;

/// <summary>
///     Extension methods for configuring KurrentDB as Orleans Journaling storage.
/// </summary>
public static class KurrentJournalingSiloBuilderExtensions
{
    /// <summary>
    ///     Configures KurrentDB as the default Orleans Journaling storage provider, which grain durable state
    ///     (<see cref="IDurableStateManager"/>, <see cref="DurableGrain"/>) uses.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configureOptions">An action to configure the provider options.</param>
    /// <returns>The silo builder.</returns>
    public static ISiloBuilder AddKurrentJournalStorageAsDefault(this ISiloBuilder builder, Action<KurrentJournalStorageOptions> configureOptions)
        => builder.AddKurrentJournalStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);

    /// <summary>
    ///     Configures KurrentDB as the default Orleans Journaling storage provider.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configureOptions">An optional action to configure the provider options.</param>
    /// <returns>The silo builder.</returns>
    public static ISiloBuilder AddKurrentJournalStorageAsDefault(this ISiloBuilder builder, Action<OptionsBuilder<KurrentJournalStorageOptions>>? configureOptions = null)
        => builder.AddKurrentJournalStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);

    /// <summary>
    ///     Configures KurrentDB as a named Orleans Journaling storage provider. In Orleans 10.4, grain durable state
    ///     always uses the default provider; a named provider is reached through the keyed
    ///     <see cref="IJournaledStateManagerFactory"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">The provider name.</param>
    /// <param name="configureOptions">An action to configure the provider options.</param>
    /// <returns>The silo builder.</returns>
    public static ISiloBuilder AddKurrentJournalStorage(this ISiloBuilder builder, string name, Action<KurrentJournalStorageOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(configureOptions);
        return builder.AddKurrentJournalStorage(name, x => x.Configure(configureOptions));
    }

    /// <summary>
    ///     Configures KurrentDB as a named Orleans Journaling storage provider. In Orleans 10.4, grain durable state
    ///     always uses the default provider; a named provider is reached through the keyed
    ///     <see cref="IJournaledStateManagerFactory"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">The provider name.</param>
    /// <param name="configureOptions">An optional action to configure the provider options.</param>
    /// <returns>The silo builder.</returns>
    public static ISiloBuilder AddKurrentJournalStorage(this ISiloBuilder builder, string name, Action<OptionsBuilder<KurrentJournalStorageOptions>>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        configureOptions?.Invoke(builder.Services.AddOptions<KurrentJournalStorageOptions>(name));
        builder.Services.ConfigureNamedOptionForLogging<KurrentJournalStorageOptions>(name);
        builder.Services.AddTransient<IConfigurationValidator>(sp =>
            new KurrentJournalStorageOptionsValidator(sp.GetRequiredService<IOptionsMonitor<KurrentJournalStorageOptions>>().Get(name), name));

        return builder.AddJournalStorage(name, sp =>
        {
            var options = sp.GetRequiredService<IOptionsMonitor<KurrentJournalStorageOptions>>().Get(name);
            return new KurrentJournalStorageProvider(options,
                                                     KurrentClientFactory.Create(options.ClientSettings, name),
                                                     sp.GetRequiredService<IOptions<JournaledStateManagerOptions>>(),
                                                     sp.GetRequiredService<ILogger<KurrentJournalStorageProvider>>());
        });
    }
}
