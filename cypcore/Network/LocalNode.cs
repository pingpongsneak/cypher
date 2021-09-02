// CYPCore by Matthew Hellyer is licensed under CC BY-NC-ND 4.0.
// To view a copy of this license, visit https://creativecommons.org/licenses/by-nc-nd/4.0

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Dawn;
using Serilog;
using CYPCore.Extensions;
using CYPCore.Models;

namespace CYPCore.Network
{
    /// <summary>
    /// 
    /// </summary>
    public interface ILocalNode
    {
        Task Broadcast(TopicType topicType, byte[] data);
        Task Broadcast(Peer[] peers, TopicType topicType, byte[] data);
        Task<Dictionary<ulong, Peer>> GetPeers();
        Task<ulong[]> Nodes();
        public IGossip Gossip { get; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class LocalNode : ILocalNode
    {
        public IGossip Gossip { get; }
        
        private readonly IGossip _gossip;
        private readonly NetworkClient _networkClient;
        private readonly ILogger _logger;

        public LocalNode(IGossip gossip, NetworkClient networkClient, ILogger logger)
        {
            _gossip = gossip;
            _networkClient = networkClient;
            _logger = logger.ForContext("SourceContext", nameof(LocalNode));
            Gossip = gossip;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="topicType"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public async Task Broadcast(TopicType topicType, byte[] data)
        {
            Guard.Argument(data, nameof(data)).NotNull();
            var peers = await GetPeers();
            if (!peers.Any())
            {
                await Broadcast(peers.Values.ToArray(), topicType, data);
                return;
            }

            await Broadcast(peers.Values.ToArray(), topicType, data);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="peers"></param>
        /// <param name="topicType"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public Task Broadcast(Peer[] peers, TopicType topicType, byte[] data)
        {
            Guard.Argument(data, nameof(data)).NotNull();
            Guard.Argument(peers, nameof(peers)).NotNull();
            _logger.Here().Information("Broadcasting {@TopicType} to nodes {@Nodes}", topicType, peers);
            try
            {
                if (peers.Any())
                {
                    var tasks = new List<Task>();
                    foreach (var peer in peers)
                    {
                        async void Action() => await _networkClient.SendAsync(data, topicType, peer.Host);
                        var t = new Task(Action);
                        t.Start();
                        tasks.Add(t);
                    }

                    Task.WaitAll(tasks.ToArray());
                }
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, "Error while bootstrapping clients");
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task<ulong[]> Nodes()
        {
            var peers = await GetPeers();
            if (peers == null) return null;
            var totalNodes = (ulong)peers.Count;
            return totalNodes != 0 ? peers.Keys.ToArray() : null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task<Dictionary<ulong, Peer>> GetPeers()
        {
            var peers = new Dictionary<ulong, Peer>();
            return peers;
        }
    }
}