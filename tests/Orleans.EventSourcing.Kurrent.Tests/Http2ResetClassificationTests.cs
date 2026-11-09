using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

using Grpc.Core;
using Grpc.Net.Client;

using KurrentDB.Client;

using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests;

/// <summary>
///     <see cref="KurrentRetry.IsTransient"/> depends on how Grpc.Net.Client reports a lost HTTP/2 connection: which gRPC
///     status each HTTP/2 error code becomes, and that the <see cref="HttpProtocolException"/> is reachable from the
///     status. These tests pin that against a real connection, so a client upgrade that changes it fails here rather
///     than silently in production.
/// </summary>
public sealed class Http2ResetClassificationTests
{
    [Theory]
    // A lost connection: retried
    [InlineData(0x0, StatusCode.Internal, true)]          // NO_ERROR
    [InlineData(0x1, StatusCode.Internal, true)]          // PROTOCOL_ERROR
    [InlineData(0x2, StatusCode.Internal, true)]          // INTERNAL_ERROR
    // The server refused the call before processing it: reported as Unavailable, so retried
    [InlineData(0x7, StatusCode.Unavailable, true)]       // REFUSED_STREAM
    // Won't clear on a retry
    [InlineData(0x3, StatusCode.Internal, false)]         // FLOW_CONTROL_ERROR
    [InlineData(0x8, StatusCode.Cancelled, false)]        // CANCEL
    [InlineData(0xb, StatusCode.ResourceExhausted, false)] // ENHANCE_YOUR_CALM
    public async Task AStreamResetIsClassifiedByItsErrorCode(int errorCode, StatusCode expectedStatus, bool expectedTransient)
    {
        var ct = TestContext.Current.CancellationToken;
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var server = RunResettingServerAsync(listener, (uint)errorCode, stop.Token);

        try
        {
            // A plain gRPC call
            using (var channel = GrpcChannel.ForAddress($"http://127.0.0.1:{port}"))
            {
                var method = new Method<byte[], byte[]>(MethodType.Unary, "test.Service", "Call", Marshallers.Create(b => b, b => b), Marshallers.Create(b => b, b => b));
                var call = channel.CreateCallInvoker().AsyncUnaryCall(method, null, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(30), cancellationToken: ct), []);

                var ex = await Assert.ThrowsAsync<RpcException>(() => call.ResponseAsync);
                Assert.Equal(expectedStatus, ex.StatusCode);
                Assert.Equal(expectedTransient, KurrentRetry.IsTransient(ex));
            }

            // The same through KurrentDB.Client, as the providers call it
            var settings = KurrentDBClientSettings.Create($"esdb://127.0.0.1:{port}?tls=false");
            settings.DefaultDeadline = TimeSpan.FromSeconds(30);
            await using var client = new KurrentClient(settings);
            EventData[] events = [new(Uuid.NewUuid(), "event", Array.Empty<byte>())];

            var clientEx = await Assert.ThrowsAsync<RpcException>(() => client.ConditionalAppendToStreamAsync("stream", StreamState.NoStream, events, ct));
            Assert.Equal(expectedStatus, clientEx.StatusCode);
            Assert.Equal(expectedTransient, KurrentRetry.IsTransient(clientEx));
        }
        finally
        {
            await stop.CancelAsync();
            listener.Stop();
            await server;
        }
    }

    /// <summary>
    ///     A minimal HTTP/2 server (prior knowledge, no TLS) that answers every request with RST_STREAM carrying
    ///     <paramref name="errorCode"/>.
    /// </summary>
    private static async Task RunResettingServerAsync(TcpListener listener, uint errorCode, CancellationToken ct)
    {
        var connections = new List<Task>();
        try
        {
            while (true)
            {
                var tcp = await listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
                connections.Add(ServeAsync(tcp, errorCode, ct));
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or SocketException or ObjectDisposedException)
        {
            // Stopped
        }

        await Task.WhenAll(connections).ConfigureAwait(false);
    }

    private static async Task ServeAsync(TcpClient tcp, uint errorCode, CancellationToken ct)
    {
        const byte Headers = 0x1, RstStream = 0x3, Settings = 0x4, Ping = 0x6, Ack = 0x1;

        try
        {
            using var _ = tcp;
            var stream = tcp.GetStream();
            await stream.ReadExactlyAsync(new byte[24], ct).ConfigureAwait(false); // Client connection preface
            await stream.WriteAsync(Frame(Settings, 0, 0, []), ct).ConfigureAwait(false);

            var header = new byte[9];
            while (true)
            {
                await stream.ReadExactlyAsync(header, ct).ConfigureAwait(false);
                var length = (header[0] << 16) | (header[1] << 8) | header[2];
                var type = header[3];
                var flags = header[4];
                var streamId = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(5)) & 0x7fffffff;
                var payload = new byte[length];
                await stream.ReadExactlyAsync(payload, ct).ConfigureAwait(false);

                if (type == Settings && (flags & Ack) == 0)
                {
                    await stream.WriteAsync(Frame(Settings, Ack, 0, []), ct).ConfigureAwait(false);
                }
                else if (type == Ping && (flags & Ack) == 0)
                {
                    await stream.WriteAsync(Frame(Ping, Ack, 0, payload), ct).ConfigureAwait(false);
                }
                else if (type == Headers)
                {
                    var reset = new byte[4];
                    BinaryPrimitives.WriteUInt32BigEndian(reset, errorCode);
                    await stream.WriteAsync(Frame(RstStream, 0, streamId, reset), ct).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or IOException or SocketException or ObjectDisposedException)
        {
            // The client closed the connection, or the test is over
        }
    }

    private static byte[] Frame(byte type, byte flags, int streamId, byte[] payload)
    {
        var frame = new byte[9 + payload.Length];
        frame[0] = (byte)(payload.Length >> 16);
        frame[1] = (byte)(payload.Length >> 8);
        frame[2] = (byte)payload.Length;
        frame[3] = type;
        frame[4] = flags;
        BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(5), streamId);
        payload.CopyTo(frame, 9);
        return frame;
    }
}
