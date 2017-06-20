//STATUS: PENDING - 4.8.0

using System.Threading.Tasks;
using Lucene.Net.Replicator.Http;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;

namespace Lucene.Net.Tests.Replicator.Http
{
    public class ReplicationServlet //: HttpServlet
    {
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ReplicationService service)
        {
            app.Run(async context =>
            {
                //await context.Response.WriteAsync("foo");
                service.Perform(context.Request, context.Response);
                //TODO: Just to make the compiler happy for now, need to figure out how to run this.
                await Task.Delay(0);
            });
        }
    }
}