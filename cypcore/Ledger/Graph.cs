// CYPCore by Matthew Hellyer is licensed under CC BY-NC-ND 4.0.
// To view a copy of this license, visit https://creativecommons.org/licenses/by-nc-nd/4.0

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Blake3;
using Collections.Pooled;
using CYPCore.Consensus;
using CYPCore.Consensus.Models;
using CYPCore.Cryptography;
using CYPCore.Extensions;
using CYPCore.Models;
using CYPCore.Network;
using CYPCore.Persistence;
using CYPCore.Serf;
using Dawn;
using MessagePack;
using Serilog;
using Block = CYPCore.Models.Block;
using Interpreted = CYPCore.Consensus.Models.Interpreted;

namespace CYPCore.Ledger
{
    public interface IGraph
    {
        VerifyResult TryAddBlockGraph(BlockGraph blockGraph);
        VerifyResult TryAddBlockGraph(byte[] blockGraphModel);
        Task<Transaction> GetTransaction(byte[] txnId);
        Task<IEnumerable<Models.Block>> GetBlocks(int skip, int take);
        Task<IEnumerable<Models.Block>> GetSafeguardBlocks();
        Task<ulong> GetHeight();
        Task<BlockHash> GetHash(ulong height);
    }

    public class Graph : IGraph
    {
        private readonly IUnitOfWork _unitOfWork;
        private readonly ILocalNode _localNode;
        private readonly ISerfClient _serfClient;
        private readonly IValidator _validator;
        private readonly ISigning _signing;
        private readonly ILogger _logger;
        private readonly PooledList<BlockGraph> _pooledBlockGraphs;
        private readonly IObservable<EventPattern<BlockGraphEventArgs>> _trackingBlockGraphAdded;
        private readonly IObservable<EventPattern<BlockGraphEventArgs>> _trackingBlockGraphCompleted;
        private const int MaxBlockGraphs = 10_000;

        protected class BlockGraphEventArgs : EventArgs
        {
            public BlockGraph BlockGraph { get; }
            public string Hash { get; }

            public BlockGraphEventArgs(BlockGraph blockGraph)
            {
                BlockGraph = blockGraph;
                Hash = blockGraph.Block.Hash;
            }
        }

        private EventHandler<BlockGraphEventArgs> _blockGraphAddedEventHandler;
        private EventHandler<BlockGraphEventArgs> _blockGraphAddCompletedEventHandler;

        public Graph(IUnitOfWork unitOfWork, ILocalNode localNode, ISerfClient serfClient, IValidator validator,
            ISigning signing, ILogger logger)
        {
            _unitOfWork = unitOfWork;
            _localNode = localNode;
            _serfClient = serfClient;
            _validator = validator;
            _signing = signing;
            _logger = logger;
            _pooledBlockGraphs = new PooledList<BlockGraph>(MaxBlockGraphs);
            _trackingBlockGraphAdded = Observable.FromEventPattern<BlockGraphEventArgs>(
                ev => _blockGraphAddedEventHandler += ev, ev => _blockGraphAddedEventHandler -= ev);
            _trackingBlockGraphCompleted = Observable.FromEventPattern<BlockGraphEventArgs>(
                ev => _blockGraphAddCompletedEventHandler += ev, ev => _blockGraphAddCompletedEventHandler -= ev);

            TryAddBlockGraphsListener();
            TryAddBlockmaniaListener();
            ReplayLastRound().SafeFireAndForget(exception => { _logger.Here().Error(exception, "Replay error"); });
        }

        /// <summary>
        /// 
        /// </summary>
        private async Task ReplayLastRound()
        {
            var blockGraphs =
                await _unitOfWork.BlockGraphRepository.WhereAsync(x =>
                    new ValueTask<bool>(x.Block.Round == GetRound() + 1));
            foreach (var blockGraph in blockGraphs)
            {
                OnBlockGraphAddComplete(new BlockGraphEventArgs(blockGraph));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="e"></param>
        protected virtual void OnBlockGraphAdd(BlockGraphEventArgs e)
        {
            var handler = _blockGraphAddedEventHandler;
            handler?.Invoke(this, e);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="e"></param>
        protected virtual void OnBlockGraphAddComplete(BlockGraphEventArgs e)
        {
            var handler = _blockGraphAddCompletedEventHandler;
            handler?.Invoke(this, e);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="blockGraphModel"></param>
        /// <returns></returns>
        public VerifyResult TryAddBlockGraph(byte[] blockGraphModel)
        {
            try
            {
                var blockGraph = MessagePackSerializer.Deserialize<BlockGraph>(blockGraphModel);
                var tryAddBlockGraph = TryAddBlockGraph(blockGraph);
                return tryAddBlockGraph;
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, ex.Message);
                return VerifyResult.Invalid;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="blockGraph"></param>
        /// <returns></returns>
        public VerifyResult TryAddBlockGraph(BlockGraph blockGraph)
        {
            Guard.Argument(blockGraph, nameof(blockGraph)).NotNull();
            try
            {
                var pooledBlockGraph = _pooledBlockGraphs.FirstOrDefault(x =>
                    x.Block.Hash == blockGraph.Block.Hash && x.Block.Node == blockGraph.Block.Node &&
                    x.Block.Round == blockGraph.Block.Round && x.Deps.Count == blockGraph.Deps.Count);
                if (pooledBlockGraph == null)
                {
                    _pooledBlockGraphs.Add(blockGraph);
                    OnBlockGraphAdd(new BlockGraphEventArgs(blockGraph));
                }
                else
                {
                    var block = MessagePackSerializer.Deserialize<Models.Block>(blockGraph.Block.Data);
                    if (pooledBlockGraph.PublicKey.Xor(block.BlockPos.PublicKey))
                    {
                        return VerifyResult.AlreadyExists;
                    }

                    _pooledBlockGraphs.Add(blockGraph);
                    OnBlockGraphAdd(new BlockGraphEventArgs(blockGraph));
                }
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, ex.Message);
                return VerifyResult.Invalid;
            }

            return VerifyResult.Succeed;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="blockGraph"></param>
        /// <returns></returns>
        private async Task<bool> SaveBlockGraph(BlockGraph blockGraph)
        {
            Guard.Argument(blockGraph, nameof(blockGraph)).NotNull();
            var verified = await _validator.VerifyBlockGraphSignatureNodeRound(blockGraph);
            if (verified == VerifyResult.UnableToVerify)
            {
                _logger.Here().Error("Unable to verify block for {@Node} and round {@Round}", blockGraph.Block.Node,
                    blockGraph.Block.Round);
                return false;
            }

            var saved = await _unitOfWork.BlockGraphRepository.PutAsync(blockGraph.ToIdentifier(), blockGraph);
            if (saved) return true;
            _logger.Here().Error("Unable to save block for {@Node} and round {@Round}", blockGraph.Block.Node,
                blockGraph.Block.Round);
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="blockGraph"></param>
        /// <returns></returns>
        private async Task<BlockGraph> SignBlockGraph(BlockGraph blockGraph)
        {
            Guard.Argument(blockGraph, nameof(blockGraph)).NotNull();
            await _signing.GetOrUpsertKeyName(_signing.DefaultSigningKeyName);
            var signature = await _signing.Sign(_signing.DefaultSigningKeyName, blockGraph.ToHash());
            var pubKey = await _signing.GetPublicKey(_signing.DefaultSigningKeyName);
            blockGraph.PublicKey = pubKey;
            blockGraph.Signature = signature;
            return blockGraph;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="skip"></param>
        /// <param name="take"></param>
        /// <returns></returns>
        public async Task<IEnumerable<Models.Block>> GetBlocks(int skip, int take)
        {
            Guard.Argument(skip, nameof(skip)).NotNegative();
            Guard.Argument(take, nameof(take)).NotNegative();
            var blocks = Enumerable.Empty<Models.Block>();
            try
            {
                blocks = await _unitOfWork.HashChainRepository.OrderByRangeAsync(x => x.Height, skip, take);
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, "Unable to get the blocks");
            }

            return blocks;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<Models.Block>> GetSafeguardBlocks()
        {
            var blocks = Enumerable.Empty<Models.Block>();
            try
            {
                var height = (int)await _unitOfWork.HashChainRepository.CountAsync() - 147;
                height = height < 0 ? 0 : height;
                blocks = await _unitOfWork.HashChainRepository.OrderByRangeAsync(proto => proto.Height, height, 147);
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, "Unable to get the safeguard blocks");
            }

            return blocks;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task<ulong> GetHeight()
        {
            ulong height = 0;

            try
            {
                height = (ulong)await _unitOfWork.HashChainRepository.CountAsync();
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, "Cannot get block height");
            }

            return height;
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        public async Task<BlockHash> GetHash(ulong height)
        {
            try
            {
                if (height == 0)
                {
                    // Get last block hash when no height is given
                    height = (ulong)await _unitOfWork.HashChainRepository.CountAsync();
                }

                var block = await _unitOfWork.HashChainRepository.GetAsync(b =>
                    new ValueTask<bool>(b.Height == height - 1));

                return new()
                {
                    Height = height,
                    Hash = block.ToHash()
                };
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, "Cannot get last block hash");
            }

            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="transactionId"></param>
        /// <returns></returns>
        public async Task<Transaction> GetTransaction(byte[] transactionId)
        {
            Guard.Argument(transactionId, nameof(transactionId)).NotNull().MaxCount(32);
            Transaction transaction = null;
            try
            {
                var blocks = await _unitOfWork.HashChainRepository.WhereAsync(x =>
                    new ValueTask<bool>(x.Txs.Any(t => t.TxnId.Xor(transactionId))));
                var firstBlock = blocks.FirstOrDefault();
                var found = firstBlock?.Txs.FirstOrDefault(x => x.TxnId.Xor(transactionId));
                if (found != null)
                {
                    transaction = found;
                }
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, "Unable tp get outputs");
            }

            return transaction;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        /// <param name="prevBlock"></param>
        /// <returns></returns>
        private BlockGraph CopyBlockGraph(byte[] block, byte[] prevBlock)
        {
            Guard.Argument(block, nameof(block)).NotNull();
            Guard.Argument(prevBlock, nameof(prevBlock)).NotNull();

            var next = MessagePackSerializer.Deserialize<Models.Block>(block);
            var prev = MessagePackSerializer.Deserialize<Models.Block>(prevBlock);
            var blockGraph = new BlockGraph
            {
                Block = new CYPCore.Consensus.Models.Block(Hasher.Hash(next.Height.ToBytes()).ToString(), _serfClient.ClientId,
                    next.Height, block),
                Prev = new CYPCore.Consensus.Models.Block
                {
                    Data = prevBlock,
                    Hash = Hasher.Hash(prev.Height.ToBytes()).ToString(),
                    Node = _serfClient.ClientId,
                    Round = prev.Height
                }
            };
            return blockGraph;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>s
        private IDisposable TryAddBlockGraphsListener()
        {
            var activityTrackSubscription = _trackingBlockGraphAdded
                .Where(data => data.EventArgs.BlockGraph.Block.Round == GetRound() + 1)
                .GroupByUntil(item => item.EventArgs.Hash, g => g.Throttle(TimeSpan.FromSeconds(1)).Take(1))
                .SelectMany(group => group.Buffer(TimeSpan.FromSeconds(1), 500)).Subscribe(async blockGraphs =>
                {
                    await Task.Run(async () =>
                    {
                        foreach (var data in blockGraphs)
                        {
                            var blockGraph = data.EventArgs.BlockGraph;
                            try
                            {
                                var copy = false;
                                copy |= blockGraph.Block.Node != _serfClient.ClientId;
                                if (!copy)
                                {
                                    var signBlockGraph = await SignBlockGraph(blockGraph);
                                    var saved = await SaveBlockGraph(signBlockGraph);
                                    if (!saved) return;
                                    await Publish(signBlockGraph);
                                }
                                else
                                {
                                    var saved = await SaveBlockGraph(blockGraph);
                                    if (!saved) return;
                                    var copyBlockGraph = CopyBlockGraph(blockGraph.Block.Data, blockGraph.Prev.Data);
                                    copyBlockGraph = await SignBlockGraph(copyBlockGraph);
                                    var savedCopy = await SaveBlockGraph(copyBlockGraph);
                                    if (!savedCopy) return;
                                    await Publish(copyBlockGraph);
                                }

                                OnBlockGraphAddComplete(new BlockGraphEventArgs(blockGraph));
                            }
                            catch (Exception)
                            {
                                _logger.Here().Error("Unable to add block for {@Node} and round {@Round}",
                                    blockGraph.Block.Node, blockGraph.Block.Round);
                            }
                        }
                    }).ConfigureAwait(false);
                }, exception => { _logger.Here().Error(exception, "Subscribe try add block graphs listener error"); });
            return Task.FromResult(activityTrackSubscription);
        }

        /// <summary>
        /// 
        /// </summary>
        private IDisposable TryAddBlockmaniaListener()
        {
            var activityTrackSubscription = _trackingBlockGraphCompleted.Delay(TimeSpan.FromSeconds(40))
                .GroupBy(g => g.EventArgs.Hash).SelectMany(blockGraph => Observable.FromAsync(async () =>
                    await _unitOfWork.BlockGraphRepository.WhereAsync(x =>
                        new ValueTask<bool>(x.Block.Round == GetRound() + 1 && x.Block.Hash.Equals(blockGraph.Key)))))
                .Subscribe(blockgraphs =>
                {
                    try
                    {
                        var nodeCount = blockgraphs.Select(n => n.Block.Node).Distinct().Count();
                        var f = (nodeCount - 1) / 3;
                        var quorum2F1 = 2 * f + 1;
                        if (nodeCount < quorum2F1) return;
                        var lastInterpreted = GetRound();
                        var config = new Config(lastInterpreted, Array.Empty<ulong>(), _serfClient.ClientId,
                            (ulong)nodeCount);
                        var blockmania = new Blockmania(config, _logger) { NodeCount = nodeCount };
                        blockmania.TrackingDelivered.Subscribe(x =>
                        {
                            Delivered(x.EventArgs.Interpreted).SafeFireAndForget();
                        });
                        foreach (var next in blockgraphs)
                        {
                            blockmania.Add(next);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.Here().Error(ex, "Process add blockmania error");
                    }
                }, exception => { _logger.Here().Error(exception, "Subscribe try add blockmania listener error"); });
            return activityTrackSubscription;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="deliver"></param>
        /// <returns></returns>
        private async Task Delivered(Interpreted deliver)
        {
            Guard.Argument(deliver, nameof(deliver)).NotNull();
            _logger.Here().Information("Delivered");
            try
            {
                var blocks = deliver.Blocks.Where(x => x.Data != null).ToArray();
                foreach (var next in blocks)
                {
                    var blockGraph = await _unitOfWork.BlockGraphRepository.GetAsync(x =>
                        new ValueTask<bool>(x.Block.Hash.Equals(next.Hash) && x.Block.Node == next.Node &&
                                            x.Block.Round == next.Round));
                    if (blockGraph == null)
                    {
                        _logger.Here()
                            .Error(
                                "Unable to find the matching block - Hash: {@Hash} Round: {@Round} from node {@Node}",
                                next.Hash, next.Round, next.Node);
                        continue;
                    }

                    await RemoveBlockGraph(blockGraph, next);
                    var block = MessagePackSerializer.Deserialize<Block>(next.Data);
                    var exists = await _validator.BlockExists(block);
                    if (exists == VerifyResult.AlreadyExists)
                    {
                        continue;
                    }

                    var verifyBlockGraphSignatureNodeRound =
                        await _validator.VerifyBlockGraphSignatureNodeRound(blockGraph);
                    if (verifyBlockGraphSignatureNodeRound == VerifyResult.Succeed)
                    {
                        var saved = await _unitOfWork.DeliveredRepository.PutAsync(block.ToIdentifier(), block);
                        if (!saved)
                        {
                            _logger.Here().Error("Unable to save the block: {@Hash}", block.Hash.ByteToHex());
                        }

                        _logger.Here().Information("Saved block to Delivered");
                        continue;
                    }

                    _logger.Here()
                        .Error("Unable to verify the node signatures - Hash: {@Hash} Round: {@Round} from node {@Node}",
                            next.Hash, next.Round, next.Node);
                }
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, "Delivered error");
            }
            finally
            {
                await DecideWinnerAsync();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private async Task DecideWinnerAsync()
        {
            try
            {
                var height = await _unitOfWork.HashChainRepository.GetBlockHeightAsync();
                var prevBlock = await _unitOfWork.HashChainRepository.GetAsync(block =>
                    new ValueTask<bool>(block.Height == (ulong)height));
                if (prevBlock == null) return;
                var deliveredBlocks = await _unitOfWork.DeliveredRepository.WhereAsync(block =>
                    new ValueTask<bool>(block.Height == (ulong)(height + 1)));
                if (deliveredBlocks.Any() != true) return;
                _logger.Here().Information("DecideWinnerAsync");
                var winners = deliveredBlocks.Where(x =>
                    x.BlockPos.Solution == deliveredBlocks.Select(n => n.BlockPos.Solution).Min()).ToArray();
                var blockWinner = winners.Length switch
                {
                    > 2 => winners.FirstOrDefault(winner =>
                        winner.BlockPos.Solution >= deliveredBlocks.Select(x => x.BlockPos.Solution).Max()),
                    _ => winners[0]
                };
                if (blockWinner != null)
                {
                    _logger.Here().Information("DecideWinnerAsync we have a winner");
                    var exists = await _validator.BlockExists(blockWinner);
                    if (exists == VerifyResult.AlreadyExists)
                    {
                        _logger.Here().Error("Block winner already exists");
                        await RemoveDeliveredBlock(blockWinner);
                        return;
                    }

                    var verifyBlockHeader = await _validator.VerifyBlock(blockWinner);
                    if (verifyBlockHeader == VerifyResult.UnableToVerify)
                    {
                        _logger.Here().Error("Unable to verify the block");
                        await RemoveDeliveredBlock(blockWinner);
                        return;
                    }

                    _logger.Here().Information("DecideWinnerAsync saving winner");
                    var saved = await _unitOfWork.HashChainRepository.PutAsync(blockWinner.ToIdentifier(), blockWinner);
                    if (!saved)
                    {
                        _logger.Here().Error("Unable to save the block winner");
                        return;
                    }
                }

                var removeDeliveredBlockTasks = new List<Task>();
                deliveredBlocks.ForEach(block => { removeDeliveredBlockTasks.Add(RemoveDeliveredBlock(block)); });
                await Task.WhenAll(removeDeliveredBlockTasks);
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, "Deciding winner Failed");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private async Task RemoveDeliveredBlock(Block block)
        {
            Guard.Argument(block, nameof(block)).NotNull();
            var removed = await _unitOfWork.DeliveredRepository.RemoveAsync(block.ToIdentifier());
            if (!removed)
            {
                _logger.Here().Error("Unable to remove potential block winner {@MerkelRoot}",
                    block.Hash);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="blockGraph"></param>
        /// <param name="next"></param>
        /// <returns></returns>
        private async Task RemoveBlockGraph(BlockGraph blockGraph, CYPCore.Consensus.Models.Block next)
        {
            _pooledBlockGraphs.Remove(blockGraph);
            var removed = await _unitOfWork.BlockGraphRepository.RemoveAsync(blockGraph.ToIdentifier());
            if (!removed)
            {
                _logger.Here()
                    .Error(
                        "Unable to remove the block graph for block - Hash: {@Hash} Round: {@Round} from node {@Node}",
                        next.Hash, next.Round, next.Node);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private ulong GetRound()
        {
            var round = GetRoundAsync().ConfigureAwait(false);
            return round.GetAwaiter().GetResult();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private async Task<ulong> GetRoundAsync()
        {
            ulong round = 0;
            try
            {
                var height = await _unitOfWork.HashChainRepository.CountAsync();
                round = (ulong)height - 1;
            }
            catch (Exception ex)
            {
                _logger.Here().Warning(ex, "Unable to get the round");
            }

            return round;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="blockGraph"></param>
        /// <returns></returns>
        private async Task Publish(BlockGraph blockGraph)
        {
            Guard.Argument(blockGraph, nameof(blockGraph)).NotNull();
            try
            {
                var peers = await _localNode.GetPeers();
                await _localNode.Broadcast(peers.Values.ToArray(), TopicType.AddBlockGraph,
                    MessagePackSerializer.Serialize(blockGraph));
            }
            catch (Exception ex)
            {
                _logger.Here().Error(ex, "Publish error");
            }
        }
    }
}