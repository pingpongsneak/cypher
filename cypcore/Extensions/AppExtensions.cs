// CYPCore by Matthew Hellyer is licensed under CC BY-NC-ND 4.0.
// To view a copy of this license, visit https://creativecommons.org/licenses/by-nc-nd/4.0

using System;
using System.IO;
using System.Net;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Autofac;
using AutofacSerilogIntegration;
using CYPCore.Models;
using CYPCore.Services;
using CYPCore.Persistence;
using CYPCore.Network;
using CYPCore.Cryptography;
using CYPCore.Helper;
using CYPCore.Ledger;
using GossipMesh.Core;
using Serilog.Extensions.Logging;

namespace CYPCore.Extensions
{
    public static class AppExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        /// <returns></returns>
        public static ContainerBuilder AddSerilog(this ContainerBuilder builder)
        {
            builder.RegisterLogger();
            return builder;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        /// <returns></returns>
        public static ContainerBuilder AddSigning(this ContainerBuilder builder)
        {
            builder.RegisterType<Signing>().As<ISigning>().InstancePerLifetimeScope();
            return builder;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        /// <returns></returns>
        public static ContainerBuilder AddMemoryPool(this ContainerBuilder builder)
        {
            builder.RegisterType<MemoryPool>().As<IMemoryPool>().SingleInstance();
            return builder;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="configuration"></param>
        /// <returns></returns>
        public static ContainerBuilder AddPosMinting(this ContainerBuilder builder, IConfiguration configuration)
        {
            // builder.Register(c =>
            // {
            //     var stakingConfigurationOptions = new StakingConfigurationOptions();
            //     configuration.Bind("Staking", stakingConfigurationOptions);
            //     var posMintingProvider = new PosMinting(c.Resolve<IGraph>(), c.Resolve<IMemoryPool>(),
            //         c.Resolve<IGossip>(), c.Resolve<IUnitOfWork>(), c.Resolve<ISigning>(), c.Resolve<IValidator>(),
            //         c.Resolve<ISync>(), stakingConfigurationOptions, c.Resolve<Serilog.ILogger>());
            //     return posMintingProvider;
            // }).As<IStartable>().SingleInstance();
            return builder;
        }

        /// <summary>
        ///     
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="configuration"></param>
        /// <returns></returns>
        public static ContainerBuilder AddUnitOfWork(this ContainerBuilder builder, IConfiguration configuration)
        {
            var dataFolder = configuration.GetSection("DataFolder");
            builder.Register(c =>
            {
                UnitOfWork unitOfWork = new(dataFolder.Value, c.Resolve<Serilog.ILogger>());
                return unitOfWork;
            }).As<IUnitOfWork>().SingleInstance();
            return builder;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        /// <returns></returns>
        public static ContainerBuilder AddGraph(this ContainerBuilder builder)
        {
            builder.RegisterType<Graph>().As<IGraph>().SingleInstance();
            return builder;
        }

        public static ContainerBuilder AddGossip(this ContainerBuilder builder, IConfiguration configuration)
        {
            builder.Register( c =>
            {
                var appConfigurationOptions = new AppConfigurationOptions();
                configuration.Bind("Node", appConfigurationOptions);
                
                var lifetime = c.Resolve<IHostApplicationLifetime>();
                var logger = new SerilogLoggerProvider(c.Resolve<Serilog.ILogger>()).CreateLogger(nameof(Gossip));

                var seedNodes = appConfigurationOptions.Gossip.SeedNodes != null
                    ? new IPEndPoint[appConfigurationOptions.Gossip.SeedNodes.Count]
                    : Array.Empty<IPEndPoint>();

                if (appConfigurationOptions.Gossip.SeedNodes != null)
                    foreach (var seedNode in appConfigurationOptions.Gossip.SeedNodes)
                    {
                        seedNodes.TryAdd(IPEndPoint.Parse(seedNode));
                    }

                var gossipService = new Gossip(IPEndPoint.Parse(appConfigurationOptions.Gossip.Listening),
                    appConfigurationOptions.Gossip.Name, seedNodes, c.Resolve<IMemberListener>(), logger);
                gossipService.StartAsync(lifetime).GetAwaiter();
                return gossipService;
            }).As<IStartable>().SingleInstance();
            return builder;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        /// <returns></returns>
        public static ContainerBuilder AddLocalNode(this ContainerBuilder builder)
        {
            builder.RegisterType<LocalNode>().As<ILocalNode>().SingleInstance();
            return builder;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        /// <returns></returns>
        public static ContainerBuilder AddValidator(this ContainerBuilder builder)
        {
            builder.Register(c =>
            {
                Validator validator = new Validator(c.Resolve<IUnitOfWork>(), c.Resolve<ISigning>(),
                    c.Resolve<Serilog.ILogger>());
                return validator;
            }).As<IValidator>().SingleInstance();
            return builder;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="services"></param>
        /// <param name="configuration"></param>
        /// <returns></returns>
        public static IServiceCollection AddDataKeysProtection(this IServiceCollection services,
            IConfiguration configuration)
        {
            AppConfigurationOptions appConfigurationOptions = new();
            configuration.Bind("Node", appConfigurationOptions);
            
            services.AddDataProtection().PersistKeysToFileSystem(new DirectoryInfo(appConfigurationOptions.Data.KeysProtectionPath))
                .SetApplicationName("cypher").SetDefaultKeyLifetime(TimeSpan.FromDays(3650));
            return services;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="configuration"></param>
        /// <returns></returns>
        public static ContainerBuilder AddSync(this ContainerBuilder builder, IConfiguration configuration)
        {
            var syncWithSeedNodesOnly = configuration.GetValue<bool>("SyncWithSeedNodesOnly");
            builder.Register(c =>
            {
                var sync = new Sync(c.Resolve<IUnitOfWork>(), c.Resolve<IValidator>(), c.Resolve<ILocalNode>(),
                    c.Resolve<NetworkClient>(), syncWithSeedNodesOnly, c.Resolve<Serilog.ILogger>());
                return sync;
            }).As<ISync>().SingleInstance();
            return builder;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="configuration"></param>
        /// <returns></returns>
        public static ContainerBuilder AddNodeMonitorService(this ContainerBuilder builder,
            IConfiguration configuration)
        {
            builder.Register(c =>
            {
                var nodeMonitorConfigurationOptions = new NodeMonitorConfigurationOptions();
                configuration.Bind(NodeMonitorConfigurationOptions.ConfigurationSectionName,
                    nodeMonitorConfigurationOptions);
                var nodeMonitorProvider =
                    new NodeMonitor(nodeMonitorConfigurationOptions, c.Resolve<Serilog.ILogger>());
                return nodeMonitorProvider;
            }).As<INodeMonitor>().InstancePerLifetimeScope();
            builder.RegisterType<NodeMonitorService>().As<IHostedService>();
            return builder;
        }
    }
}