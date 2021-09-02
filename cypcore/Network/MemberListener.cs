using System.Threading.Tasks;
using GossipMesh.Core;

namespace CYPCore.Network
{
    public class MemberListener: IMemberListener
    {
        public Task MemberUpdatedCallback(MemberEvent memberEvent)
        {
            throw new System.NotImplementedException();
        }
    }
}