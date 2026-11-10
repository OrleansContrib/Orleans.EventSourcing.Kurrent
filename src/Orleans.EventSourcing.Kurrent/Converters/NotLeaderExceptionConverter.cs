using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Converters;

[GenerateSerializer, Alias("Orleans.EventSourcing.Kurrent.Converters.NotLeaderExceptionSurrogate"), Immutable]
internal readonly record struct NotLeaderExceptionSurrogate(string Host, int Port);

[RegisterConverter]
internal sealed class NotLeaderExceptionConverter : IConverter<NotLeaderException, NotLeaderExceptionSurrogate>
{
    public NotLeaderException ConvertFromSurrogate(in NotLeaderExceptionSurrogate surrogate) => new(surrogate.Host, surrogate.Port);

    public NotLeaderExceptionSurrogate ConvertToSurrogate(in NotLeaderException value) => new(value.LeaderEndpoint.Host, value.LeaderEndpoint.Port);
}
