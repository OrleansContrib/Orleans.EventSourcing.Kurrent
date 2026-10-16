using KurrentDB.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

using Orleans.EventSourcing.Kurrent.Configuration;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Factory used to create instances of KurrentClient
/// </summary>
internal static class KurrentClientFactory
{
    public static Uri InMemoryEmulatorUri { get; } = new Uri("https://kurrentemulator:2113/");
    public static Uri InMemoryEmulatorUriInsecure { get; } = new Uri("http://kurrentemulator:2113/");
    
    /// <summary>
    ///     Creates a KurrentClient instance.
    /// </summary>
    public static IKurrentClient Create(IServiceProvider serviceProvider, string name)
        => Create(serviceProvider.GetRequiredService<IOptionsMonitor<KurrentStorageOptions>>().Get(name).ClientSettings, name);

    /// <summary>
    ///     Creates a KurrentClient instance from explicit settings. <paramref name="name"/> selects the shared in-memory
    ///     emulator instance when the settings point at the emulator address.
    /// </summary>
    public static IKurrentClient Create(KurrentDBClientSettings clientSettings, string name)
    {
        ArgumentNullException.ThrowIfNull(clientSettings);

        if (InMemoryEmulatorUri.Equals(clientSettings.ConnectivitySettings.Address) ||
            InMemoryEmulatorUriInsecure.Equals(clientSettings.ConnectivitySettings.Address))
        {
            return InMemoryKurrentClient.Get(name);
        }
        else
        {
            return new KurrentClient(clientSettings);
        }
    }

    public static IKurrentClient Create(IServiceProvider serviceProvider)
    {
        var clientSettings = serviceProvider.GetRequiredService<IOptions<KurrentClusteringOptions>>().Value.ClientSettings;

        if (InMemoryEmulatorUri.Equals(clientSettings.ConnectivitySettings.Address) ||
            InMemoryEmulatorUriInsecure.Equals(clientSettings.ConnectivitySettings.Address))
        {
            return InMemoryKurrentClient.Get(string.Empty);
        }
        else
        {
            return new KurrentClient(clientSettings);
        }
    }
}
