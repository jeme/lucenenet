using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Replicator.Http;
using Microsoft.AspNetCore.Http;

namespace Lucene.Net.Replicator.AspNetCore
{
    public class AspNetCoreReplicationRequest : IReplicationRequest
    {
        private readonly HttpRequest request;

        public AspNetCoreReplicationRequest(HttpRequest request)
        {
            this.request = request;
        }

        public string Path { get { return request.PathBase + request.Path; } }

        public bool ContainsQueryParam(string name)
        {
            return request.Query.ContainsKey(name);
        }

        public string QueryParam(string name)
        {
            return request.Query[name].SingleOrDefault();
        }
    }
}
