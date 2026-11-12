using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Converters;

[GenerateSerializer, Alias("Orleans.EventSourcing.Kurrent.Converters.DiscoveryExceptionSurrogate"), Immutable]
internal readonly record struct DiscoveryExceptionSurrogate(int MaxDiscoverAttempts);

[RegisterConverter]
internal sealed class DiscoveryExceptionConverter : IConverter<DiscoveryException, DiscoveryExceptionSurrogate>
{
    public DiscoveryException ConvertFromSurrogate(in DiscoveryExceptionSurrogate surrogate) => new(surrogate.MaxDiscoverAttempts);

    public DiscoveryExceptionSurrogate ConvertToSurrogate(in DiscoveryException value) => new(value.MaxDiscoverAttempts);
}
