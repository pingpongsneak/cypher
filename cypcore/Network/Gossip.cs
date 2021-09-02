using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Autofac;
using GossipMesh.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NetMQ;
using NetMQ.Sockets;

namespace CYPCore.Network
{
    public interface IGossip: IStartable
    {
        string Name { get; }
        ulong Id { get; }
        IPEndPoint NodeIP { get; }
        IPEndPoint[] Seeds { get; }
        Task StartAsync(IHostApplicationLifetime applicationLifetime);
    }

    public class Gossip: IGossip
    {
        public string Name { get; }
        public ulong Id { get; }
        public IPEndPoint NodeIP { get; }
        public IPEndPoint[] Seeds { get; }

        private Gossiper _gossiper;
        private readonly IMemberListener _memberListener;
        private readonly ILogger _logger;
        
        public Gossip(IPEndPoint ipNode, string name, IPEndPoint[] seeds, IMemberListener memberListener, ILogger logger)
        {
            NodeIP = ipNode;
            Name = name;
            Seeds = seeds;
            _memberListener = memberListener;
            _logger = logger;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="applicationLifetime"></param>
        public async Task StartAsync(IHostApplicationLifetime applicationLifetime)
        {
            var running = false;
            _gossiper = await StartGossiperAsync();

            if (_gossiper != null)
            {
                running = true;
            }
            
            using var response = new ResponseSocket();
            
            Console.WriteLine("Binding tcp://{0}:{1}", NodeIP.Address, NodeIP.Port);
            response.Bind($"tcp://{NodeIP.Address}:{NodeIP.Port}");
            
            applicationLifetime.ApplicationStopping.Register(() =>
            {
                try
                {
                    running = false;
                    _gossiper?.Dispose();
                }
                catch (Exception)
                {
                    // ignored
                }
            });
            
            while (running)
            {
                var msg = response.ReceiveFrameString(out var hasMore);
                Console.WriteLine("Msg received! {0}", msg);
                response.SendFrame(msg, hasMore);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private async Task<Gossiper> StartGossiperAsync()
        {
            var options = new GossiperOptions { SeedMembers = Seeds, MemberListeners = new List<IMemberListener>{ _memberListener}};
            var gossiper = new Gossiper((ushort)NodeIP.Port, 0x06, (ushort)NodeIP.Port, options, _logger);
            await gossiper.StartAsync().ConfigureAwait(false);
            return gossiper;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            // Ignore
        }
    }
}