// CYPCore by Matthew Hellyer is licensed under CC BY-NC-ND 4.0.
// To view a copy of this license, visit https://creativecommons.org/licenses/by-nc-nd/4.0

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Dawn;
using Serilog;
using CYPCore.Extensions;
using CYPCore.Serf;
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
        public ISerfClientV2 SerfClient { get; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class LocalNode : ILocalNode
    {
        private readonly ISerfClientV2 _serfClient;
        private readonly NetworkClient _networkClient;
        private readonly ILogger _logger;
        private TcpSession _tcpSession;

        public LocalNode(ISerfClientV2 serfClient, NetworkClient networkClient, ILogger logger)
        {
            _serfClient = serfClient;
            _networkClient = networkClient;
            _logger = logger.ForContext("SourceContext", nameof(LocalNode));
            SerfClient = _serfClient;
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
                // TODO: Temporary solution until we remove or fix Serf.
                // Hard coding default ports for now as this won't cause any issues as long as we have some of the seed nodes with these settings.  
                var seedPeers = _serfClient.SeedNodes.Seeds.Select(x => new Peer {Host = x.Replace("7946", "7000")});
                await Broadcast(seedPeers.ToArray(), topicType, data);
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
                        var t = new Task(async () => await _networkClient.SendAsync(data, topicType, peer.Host));
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
        public Task<Dictionary<ulong, Peer>> GetPeers()
        {
            var tcs = new TaskCompletionSource<Dictionary<ulong, Peer>>();
            
            try
            {
                _serfClient.GetMembers(members =>
                {
                    var peers = new Dictionary<ulong, Peer>();
                    foreach (var member in members.Members.Where(member =>
                        _serfClient.Name != member.Name &&
                        member.Status == "alive" &&
                        member.Tags.ContainsKey("pubkey") &&
                        member.Tags.ContainsKey("rest")))
                    {
                        try
                        {
                            if (_serfClient.ClientId == Helper.Util.HashToId(member.Tags["pubkey"])) continue;
                            member.Tags.TryGetValue("rest", out var restEndpoint);
                            if (string.IsNullOrEmpty(restEndpoint)) continue;
                            if (!Uri.TryCreate($"{restEndpoint}", UriKind.Absolute, out var uri)) continue;
                            if (uri.Host is "0.0.0.0" or "::0")
                            {
                                continue;
                            }

                            member.Tags.TryGetValue("nodeversion", out var nodeVersion);

                            var peer = new Peer
                            {
                                Host = uri.OriginalString,
                                ClientId = Helper.Util.HashToId(member.Tags["pubkey"]),
                                PublicKey = member.Tags["pubkey"],
                                NodeName = member.Name,
                                NodeVersion = nodeVersion ?? string.Empty
                            };
                            if (peers.ContainsKey(peer.ClientId)) continue;
                            if (peers.TryAdd(peer.ClientId, peer)) continue;
                            _logger.Here().Error("Failed adding or exists in remote nodes: {@Node}", member.Name);
                        }
                        catch (Exception ex)
                        {
                            _logger.Here().Error(ex, "Error reading member");
                        }
                    }
                    tcs.SetResult(peers);
                });
                
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, "Error reading members");
                tcs.SetResult(null);
            }
            
            return tcs.Task;
        }

        public ISerfClientV2 SerfClient { get; }
    }
}