using System.IO;

namespace Lucene.Net.Replicator.Http
{
    /// <summary>
    /// 
    /// </summary>
    /// <remarks>
    /// .NET Specific Abstraction  
    /// </remarks>
    //Note: LUCENENET specific
    public interface IReplicationResponse
    {
        int StatusCode { get; set; }
        Stream Body { get; }
        void Flush();
    }
}