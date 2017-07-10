namespace Lucene.Net.Replicator.Http
{
    /// <summary>
    /// 
    /// </summary>
    /// <remarks>
    /// .NET Specific Abstraction  
    /// </remarks>
    public interface IReplicationRequest
    {
        string Path { get; }
        bool ContainsQueryParam(string name);
        string QueryParam(string name);
    }
}