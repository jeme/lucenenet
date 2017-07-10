using Lucene.Net.Replicator.Http;
using Microsoft.AspNetCore.Http;

namespace Lucene.Net.Replicator.AspNetCore
{
    //Note: LUCENENET specific
    public static class AspNetCoreReplicationServiceExtentions
    {
        public static void Perform(this ReplicationService self, HttpRequest request, HttpResponse response)
        {
            self.Perform(new AspNetCoreReplicationRequest(request), new AspNetCoreReplicationResponse(response));
        }
    }
}