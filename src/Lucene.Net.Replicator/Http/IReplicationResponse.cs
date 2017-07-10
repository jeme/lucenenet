using System.IO;

namespace Lucene.Net.Replicator.Http
{
    /// <summary>
    /// 
    /// </summary>
    /// <remarks>
    /// .NET Specific Abstraction  
    /// </remarks>
    public interface IReplicationResponse
    {
        int StatusCode { get; set; }
        Stream Body { get; }
        void Flush();
    }
}