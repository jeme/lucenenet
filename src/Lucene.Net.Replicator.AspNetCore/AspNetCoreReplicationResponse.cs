using System.IO;
using Lucene.Net.Replicator.Http;
using Microsoft.AspNetCore.Http;

namespace Lucene.Net.Replicator.AspNetCore
{
    public class AspNetCoreReplicationResponse : IReplicationResponse
    {
        private readonly HttpResponse response;

        public AspNetCoreReplicationResponse(HttpResponse response)
        {
            this.response = response;
        }

        public int StatusCode
        {
            get { return response.StatusCode; }
            set { response.StatusCode = value; }
        }

        public Stream Body { get { return response.Body; } }

        public void Flush()
        {
            response.Body.Flush();
        }
    }
}