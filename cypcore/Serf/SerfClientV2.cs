using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using CYPCore.Serf.Message;
using MessagePack;

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
        public bool IsActive { get; }
    }
    
    /// <summary>
    /// 
    /// </summary>
    public class SerfClientV2: ISerfClientV2
    {
        public bool IsActive { get; private set; }
        private TcpClient _tcpClient;
        private NetworkStream _stream;
        private long _sequence;
        private IPEndPoint _endpoint;
        private readonly List<IMessageHandler> _handlers = new();

        /// <summary>
        /// 
        /// </summary>
        public void Join(IEnumerable<string> members)
        {
            var header = new RequestHeader {Command = SerfCommandLine.Join, Sequence = GetSeq()};
            var join = new JoinRequest {Existing = members.ToArray()};
            Send(header, join);
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
        private void Send(RequestHeader requestHeader, JoinRequest joinRequest)
        {
            var headerBytes = MessagePackSerializer.Serialize(requestHeader);
            var commandBytes = MessagePackSerializer.Serialize(joinRequest);
            var instructionBytes = headerBytes.Concat(commandBytes).ToArray();

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