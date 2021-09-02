using System.Collections.Generic;

namespace CYPCore.Models
{
    public class AppConfigurationOptions
    {
        public WebApiOptions WebApi { get; set; }
        public GossipOptions Gossip { get; set; }
        public DataOptions Data { get; set; }
    }

    public class WebApiOptions
    {
        public string Listening { get; set; }
        public string Advertise { get; set; }
    }

    public class GossipOptions
    {
        public string Listening { get; set; }
        public List<string> SeedNodes { get; set; }
        public bool SyncOnlyWithSeedNodes { get; set; }
        public string Name { get; set; }
    }

    public class DataOptions
    {
        public string RocksDb { get; set; }
        public string KeysProtectionPath { get; set; }
    }
}
