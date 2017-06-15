using System;
using System.Collections.Generic;

namespace org.apache.lucene.replicator
{

	using DirectoryReader = org.apache.lucene.index.DirectoryReader;
	using IndexCommit = org.apache.lucene.index.IndexCommit;
	using ReplicationHandler = org.apache.lucene.replicator.ReplicationClient.ReplicationHandler;
	using Directory = org.apache.lucene.store.Directory;
	using IOContext = org.apache.lucene.store.IOContext;
	using InfoStream = org.apache.lucene.util.InfoStream;

	public class IndexAndTaxonomyReplicationHandler : ReplicationHandler
	{
	  public const string INFO_STREAM_COMPONENT = "IndexAndTaxonomyReplicationHandler";
	  private readonly Directory indexDir;
	  private readonly Directory taxoDir;
	  private readonly Callable<bool?> callback;
//JAVA TO C# CONVERTER NOTE: Fields cannot have the same name as methods:
	  private volatile IDictionary<string, IList<RevisionFile>> currentRevisionFiles_Renamed;
//JAVA TO C# CONVERTER NOTE: Fields cannot have the same name as methods:
	  private volatile string currentVersion_Renamed;
	  private volatile InfoStream infoStream = InfoStream.Default;

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public IndexAndTaxonomyReplicationHandler(org.apache.lucene.store.Directory indexDir, org.apache.lucene.store.Directory taxoDir, java.util.concurrent.Callable<Nullable<bool>> callback) throws java.io.IOException
	  public IndexAndTaxonomyReplicationHandler(Directory indexDir, Directory taxoDir, Callable<bool?> callback)
	  {
		this.callback = callback;
		this.indexDir = indexDir;
		this.taxoDir = taxoDir;
		currentRevisionFiles_Renamed = null;
		currentVersion_Renamed = null;
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final boolean indexExists = org.apache.lucene.index.DirectoryReader.indexExists(indexDir);
		bool indexExists = DirectoryReader.indexExists(indexDir);
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final boolean taxoExists = org.apache.lucene.index.DirectoryReader.indexExists(taxoDir);
		bool taxoExists = DirectoryReader.indexExists(taxoDir);
		if (indexExists != taxoExists)
		{
		  throw new System.InvalidOperationException("search and taxonomy indexes must either both exist or not: index=" + indexExists + " taxo=" + taxoExists);
		}
		if (indexExists)
		{ // both indexes exist
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.apache.lucene.index.IndexCommit indexCommit = IndexReplicationHandler.getLastCommit(indexDir);
		  IndexCommit indexCommit = IndexReplicationHandler.getLastCommit(indexDir);
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.apache.lucene.index.IndexCommit taxoCommit = IndexReplicationHandler.getLastCommit(taxoDir);
		  IndexCommit taxoCommit = IndexReplicationHandler.getLastCommit(taxoDir);
		  currentRevisionFiles_Renamed = IndexAndTaxonomyRevision.revisionFiles(indexCommit, taxoCommit);
		  currentVersion_Renamed = IndexAndTaxonomyRevision.revisionVersion(indexCommit, taxoCommit);
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.apache.lucene.util.InfoStream infoStream = org.apache.lucene.util.InfoStream.getDefault();
		  InfoStream infoStream = InfoStream.Default;
		  if (infoStream.isEnabled(INFO_STREAM_COMPONENT))
		  {
			infoStream.message(INFO_STREAM_COMPONENT, "constructor(): currentVersion=" + currentVersion_Renamed + " currentRevisionFiles=" + currentRevisionFiles_Renamed);
			infoStream.message(INFO_STREAM_COMPONENT, "constructor(): indexCommit=" + indexCommit + " taxoCommit=" + taxoCommit);
		  }
		}
	  }
	  public override string currentVersion()
	  {
		return currentVersion_Renamed;
	  }
	  public override IDictionary<string, IList<RevisionFile>> currentRevisionFiles()
	  {
		return currentRevisionFiles_Renamed;
	  }
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public void revisionReady(String version, java.util.Map<String,java.util.List<RevisionFile>> revisionFiles, java.util.Map<String,java.util.List<String>> copiedFiles, java.util.Map<String,org.apache.lucene.store.Directory> sourceDirectory) throws java.io.IOException
	  public override void revisionReady(string version, IDictionary<string, IList<RevisionFile>> revisionFiles, IDictionary<string, IList<string>> copiedFiles, IDictionary<string, Directory> sourceDirectory)
	  {
		Directory taxoClientDir = sourceDirectory[IndexAndTaxonomyRevision.TAXONOMY_SOURCE];
		Directory indexClientDir = sourceDirectory[IndexAndTaxonomyRevision.INDEX_SOURCE];
		IList<string> taxoFiles = copiedFiles[IndexAndTaxonomyRevision.TAXONOMY_SOURCE];
		IList<string> indexFiles = copiedFiles[IndexAndTaxonomyRevision.INDEX_SOURCE];
		string taxoSegmentsFile = IndexReplicationHandler.getSegmentsFile(taxoFiles, true);
		string indexSegmentsFile = IndexReplicationHandler.getSegmentsFile(indexFiles, false);
		bool success = false;
		try
		{
		  IndexReplicationHandler.copyFiles(taxoClientDir, taxoDir, taxoFiles);
		  IndexReplicationHandler.copyFiles(indexClientDir, indexDir, indexFiles);
		  if (taxoFiles.Count > 0)
		  {
			taxoDir.sync(taxoFiles);
		  }
		  indexDir.sync(indexFiles);
		  if (!string.ReferenceEquals(taxoSegmentsFile, null))
		  {
			taxoClientDir.copy(taxoDir, taxoSegmentsFile, taxoSegmentsFile, IOContext.READONCE);
		  }
		  indexClientDir.copy(indexDir, indexSegmentsFile, indexSegmentsFile, IOContext.READONCE);

		  if (!string.ReferenceEquals(taxoSegmentsFile, null))
		  {
			taxoDir.sync(Collections.singletonList(taxoSegmentsFile));
		  }
		  indexDir.sync(Collections.singletonList(indexSegmentsFile));

		  success = true;
		}
		finally
		{
		  if (!success)
		  {
			taxoFiles.Add(taxoSegmentsFile); // add it back so it gets deleted too
			IndexReplicationHandler.cleanupFilesOnFailure(taxoDir, taxoFiles);
			indexFiles.Add(indexSegmentsFile); // add it back so it gets deleted too
			IndexReplicationHandler.cleanupFilesOnFailure(indexDir, indexFiles);
		  }
		}
		currentRevisionFiles_Renamed = revisionFiles;
		currentVersion_Renamed = version;
		if (infoStream.isEnabled(INFO_STREAM_COMPONENT))
		{
		  infoStream.message(INFO_STREAM_COMPONENT, "revisionReady(): currentVersion=" + currentVersion_Renamed + " currentRevisionFiles=" + currentRevisionFiles_Renamed);
		}
		IndexReplicationHandler.writeSegmentsGen(taxoSegmentsFile, taxoDir);
		IndexReplicationHandler.writeSegmentsGen(indexSegmentsFile, indexDir);
		IndexReplicationHandler.cleanupOldIndexFiles(indexDir, indexSegmentsFile);
		IndexReplicationHandler.cleanupOldIndexFiles(taxoDir, taxoSegmentsFile);
		if (callback != null)
		{
		  try
		  {
			callback.call();
		  }
		  catch (Exception e)
		  {
			throw new IOException(e);
		  }
		}
	  }
	  public virtual InfoStream InfoStream
	  {
		  set
		  {
			if (value == null)
			{
			  value = InfoStream.NO_OUTPUT;
			}
			this.infoStream = value;
		  }
	  }
	}
}