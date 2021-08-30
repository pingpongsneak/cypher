using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using CYPCore.Cryptography;
using CYPCore.Extensions;
using CYPCore.Helper;
using CYPCore.Models;
using CYPCore.Serf.Message;
using MessagePack;
using Serilog;

namespace CYPCore.Serf
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal struct MessageHandler<T> : IMessageHandler
    {
        public ulong Seq { get; set; }
        public Action<T> Handler;
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        public void Process(MemoryStream stream)
        {
            try
            {
                var response = MessagePackSerializer.Deserialize<T>(stream);
                Handler(response);
            }
            catch (MessagePackSerializationException ex)
            {
                if (ex.InnerException is not EndOfStreamException)
                {
                    if (ex.InnerException != null) throw;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
    }

    /// <summary>
    /// 
    /// </summary>
    internal interface IMessageHandler
    {
        ulong Seq { get; }
        void Process(MemoryStream stream);
    }

    /// <summary>
    /// 
    /// </summary>
    public interface ISerfClientV2
    {
        ulong ClientId { get; }
        string ProcessError { get; set; }
        bool ProcessStarted { get; set; }
        int ProcessId { get; set; }
        string Name { get; set; }
        SerfConfigurationOptions SerfConfigurationOptions { get; }
        ApiConfigurationOptions ApiConfigurationOptions { get; }
        SerfSeedNodes SeedNodes { get; }
        public bool IsActive { get; }
        void GetMembers(Action<MembersResponse> action);
        Task<int> MembersCount();
        void Join(IEnumerable<string> members, Action<JoinResponse> action);
        void Connect(IPEndPoint endpoint, TimeSpan timeout = default);
    }
    
    /// <summary>
    /// 
    /// </summary>
    public class SerfClientV2: ISerfClientV2
    {
        public ulong ClientId { get; private set; }
        public string ProcessError { get; set; }
        public bool ProcessStarted { get; set; }
        public int ProcessId { get; set; }
        public string Name { get; set; }
        public SerfConfigurationOptions SerfConfigurationOptions { get; }
        public ApiConfigurationOptions ApiConfigurationOptions { get; }
        public SerfSeedNodes SeedNodes { get; }
        public bool IsActive { get; private set; }
        private TcpClient _tcpClient;
        private NetworkStream _stream;
        private long _sequence;
        private IPEndPoint _endpoint;
        private readonly List<IMessageHandler> _handlers = new();
        private readonly ISigning _signing;
        private readonly ILogger _logger;

        public SerfClientV2(ISigning signing, SerfConfigurationOptions serfConfigurationOptions,
            ApiConfigurationOptions apiConfigurationOptions, SerfSeedNodes seedNodes, ILogger logger)
        {
            _signing = signing;
            _logger = logger.ForContext("SourceContext", nameof(SerfClient));

            SerfConfigurationOptions = serfConfigurationOptions;
            ApiConfigurationOptions = apiConfigurationOptions;
            SeedNodes = seedNodes;
            
            SetClientId().SafeFireAndForget(exception => { _logger.Here().Error(exception, "Setting client id error"); });
        }
        
        /// <summary>
        /// 
        /// </summary>
        public void Join(IEnumerable<string> members, Action<JoinResponse> action)
        {
            var header = new RequestHeader {Command = SerfCommandLine.Join, Sequence = GetSeq()};
            var join = new JoinRequest {Existing = members.ToArray()};
            Send(header, join, action);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Task<int> MembersCount()
        {
            var tcs = new TaskCompletionSource<int>();
            GetMembers(members =>
            {
                tcs.SetResult(members.Members.Count() - 1);
            });

            return tcs.Task;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="action"></param>
        public void GetMembers(Action<MembersResponse> action)
        {
            var header = new RequestHeader
            {
                Command = SerfCommandLine.Members,
                Sequence = GetSeq()
            };
            
            Send(header, action);
        }
        
        /// <summary>
        /// 
        /// </summary>
        public async Task SetClientId()
        {
            ulong clientId;

            try
            {
                var pubKey = await _signing.GetPublicKey(_signing.DefaultSigningKeyName);
                ClientId = Util.HashToId(pubKey.ByteToHex());
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, "Cannot get client ID");
            }
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private ulong GetSeq()
        {
            return (ulong)Interlocked.Increment(ref _sequence);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="header"></param>
        /// <param name="action"></param>
        /// <typeparam name="TResponse"></typeparam>
        private void Register<TResponse>(RequestHeader header, Action<TResponse> action)
        {
            var handler = new MessageHandler<TResponse> {Seq = header.Sequence, Handler = action};
            _handlers.Add(handler);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private async Task ReadAsync()
        {
            IsActive = true;
            var buffer = new byte[8192];
            await using var stream = new MemoryStream();
            while (IsActive)
            {
                try
                {
                    var length = await _stream.ReadAsync(buffer.AsMemory(0, 8192));
                    if (length == 0)
                    {
                        Disconnect();
                        return;
                    }

                    stream.Write(buffer, 0, length);
                    stream.Seek(0, SeekOrigin.Begin);
                    while (stream.Position < stream.Length) Process(stream);
                    stream.Position = 0;
                }
                catch (IOException)
                {
                    Disconnect();
                    return;
                }
            }
        }
        
        /// <summary>
        /// 
        /// </summary>
        private void Disconnect()
        {
            IsActive = false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        private void Process(MemoryStream stream)
        {
            try
            {
                var header = MessagePackSerializer.Deserialize<ResponseHeader>(stream);
                if (!string.IsNullOrEmpty(header.Error))
                {
                    return;
                }

                for (var i = 0; i < _handlers.Count; ++i)
                {
                    var handler = _handlers[i];
                    if (handler.Seq != header.Seq) continue;
                    _handlers.RemoveAt(i);
                    handler.Process(stream);
                    break;
                }
            }
            catch (MessagePackSerializationException ex)
            {
                if (ex.InnerException is not EndOfStreamException)
                {
                    if (ex.InnerException != null) throw;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="timeout"></param>
        public async void Connect(IPEndPoint endpoint, TimeSpan timeout = default)
        {
            _endpoint = endpoint;
            if (timeout == default) timeout = TimeSpan.FromSeconds(10);
            _tcpClient = new TcpClient
            {
                SendTimeout = (int) timeout.TotalMilliseconds, ReceiveTimeout = (int) timeout.TotalMilliseconds
            };
            await _tcpClient.ConnectAsync(_endpoint.Address, _endpoint.Port);
            _stream = _tcpClient.GetStream();
            Handshake();
            await ReadAsync();
        }
        
        /// <summary>
        /// 
        /// </summary>
        private void Handshake()
        {
            var header = new RequestHeader {Command = SerfCommandLine.Handshake, Sequence = GetSeq()};
            var handshake = new Handshake {Version = 1};
            
            Send(header, handshake);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="requestHeader"></param>
        /// <param name="handshake"></param>
        private void Send(RequestHeader requestHeader, Handshake handshake)
        {
            var headerBytes = MessagePackSerializer.Serialize(requestHeader);
            var commandBytes = MessagePackSerializer.Serialize(handshake);
            var instructionBytes = headerBytes.Concat(commandBytes).ToArray();

            _stream.WriteAsync(instructionBytes, 0, instructionBytes.Length);
            _stream.Flush();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="requestHeader"></param>
        /// <param name="joinRequest"></param>
        private void Send(RequestHeader requestHeader, JoinRequest joinRequest, Action<JoinResponse> action)
        {
            var headerBytes = MessagePackSerializer.Serialize(requestHeader);
            var commandBytes = MessagePackSerializer.Serialize(joinRequest);
            var instructionBytes = headerBytes.Concat(commandBytes).ToArray();

            Register(requestHeader, action);
            
            _stream.WriteAsync(instructionBytes, 0, instructionBytes.Length);
            _stream.Flush();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="requestHeader"></param>
        private void Send(RequestHeader requestHeader)
        {
            var headerBytes = MessagePackSerializer.Serialize(requestHeader);
            _stream.WriteAsync(headerBytes, 0, headerBytes.Length);
            _stream.Flush();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="header"></param>
        /// <param name="action"></param>
        /// <typeparam name="TResponse"></typeparam>
        private void Send<TResponse>(RequestHeader header,  Action<TResponse> action)
        {
            Register(header, action);
            Send(header);
        }
    }
    
}