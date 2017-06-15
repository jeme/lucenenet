using System;
using System.Diagnostics;
using System.Collections.Generic;

namespace org.apache.lucene.replicator
{
	/*
	 * Licensed to the Apache Software Foundation (ASF) under one or more
	 * contributor license agreements.  See the NOTICE file distributed with
	 * this work for additional information regarding copyright ownership.
	 * The ASF licenses this file to You under the Apache License, Version 2.0
	 * (the "License"); you may not use this file except in compliance with
	 * the License.  You may obtain a copy of the License at
	 *
	 *     http://www.apache.org/licenses/LICENSE-2.0
	 *
	 * Unless required by applicable law or agreed to in writing, software
	 * distributed under the License is distributed on an "AS IS" BASIS,
	 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	 * See the License for the specific language governing permissions and
	 * limitations under the License.
	 */


	using DirectoryTaxonomyWriter = org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
	using TaxonomyWriterCache = org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;
	using IndexCommit = org.apache.lucene.index.IndexCommit;
	using IndexDeletionPolicy = org.apache.lucene.index.IndexDeletionPolicy;
	using IndexWriter = org.apache.lucene.index.IndexWriter;
	using IndexWriterConfig = org.apache.lucene.index.IndexWriterConfig;
	using OpenMode = org.apache.lucene.index.IndexWriterConfig.OpenMode;
	using SnapshotDeletionPolicy = org.apache.lucene.index.SnapshotDeletionPolicy;
	using Directory = org.apache.lucene.store.Directory;
	using IOContext = org.apache.lucene.store.IOContext;

	/// <summary>
	/// A <seealso cref="Revision"/> of a single index and taxonomy index files which comprises
	/// the list of files from both indexes. This revision should be used whenever a
	/// pair of search and taxonomy indexes need to be replicated together to
	/// guarantee consistency of both on the replicating (client) side.
	/// </summary>
	/// <seealso cref= IndexRevision
	/// 
	/// @lucene.experimental </seealso>
	public class IndexAndTaxonomyRevision : Revision
	{

	  /// <summary>
	  /// A <seealso cref="DirectoryTaxonomyWriter"/> which sets the underlying
	  /// <seealso cref="IndexWriter"/>'s <seealso cref="IndexDeletionPolicy"/> to
	  /// <seealso cref="SnapshotDeletionPolicy"/>.
	  /// </summary>
	  public sealed class SnapshotDirectoryTaxonomyWriter : DirectoryTaxonomyWriter
	  {

		internal SnapshotDeletionPolicy sdp;
		internal IndexWriter writer;

		/// <seealso cref= DirectoryTaxonomyWriter#DirectoryTaxonomyWriter(Directory,
		///      IndexWriterConfig.OpenMode, TaxonomyWriterCache) </seealso>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public SnapshotDirectoryTaxonomyWriter(org.apache.lucene.store.Directory directory, org.apache.lucene.index.IndexWriterConfig.OpenMode openMode, org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache cache) throws java.io.IOException
		public SnapshotDirectoryTaxonomyWriter(Directory directory, IndexWriterConfig.OpenMode openMode, TaxonomyWriterCache cache) : base(directory, openMode, cache)
		{
		}

		/// <seealso cref= DirectoryTaxonomyWriter#DirectoryTaxonomyWriter(Directory, IndexWriterConfig.OpenMode) </seealso>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public SnapshotDirectoryTaxonomyWriter(org.apache.lucene.store.Directory directory, org.apache.lucene.index.IndexWriterConfig.OpenMode openMode) throws java.io.IOException
		public SnapshotDirectoryTaxonomyWriter(Directory directory, IndexWriterConfig.OpenMode openMode) : base(directory, openMode)
		{
		}

		/// <seealso cref= DirectoryTaxonomyWriter#DirectoryTaxonomyWriter(Directory) </seealso>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public SnapshotDirectoryTaxonomyWriter(org.apache.lucene.store.Directory d) throws java.io.IOException
		public SnapshotDirectoryTaxonomyWriter(Directory d) : base(d)
		{
		}

		protected internal override IndexWriterConfig createIndexWriterConfig(IndexWriterConfig.OpenMode openMode)
		{
		  IndexWriterConfig conf = base.createIndexWriterConfig(openMode);
		  sdp = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
		  conf.IndexDeletionPolicy = sdp;
		  return conf;
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override protected org.apache.lucene.index.IndexWriter openIndexWriter(org.apache.lucene.store.Directory directory, org.apache.lucene.index.IndexWriterConfig config) throws java.io.IOException
		protected internal override IndexWriter openIndexWriter(Directory directory, IndexWriterConfig config)
		{
		  writer = base.openIndexWriter(directory, config);
		  return writer;
		}

		/// <summary>
		/// Returns the <seealso cref="SnapshotDeletionPolicy"/> used by the underlying <seealso cref="IndexWriter"/>. </summary>
		public SnapshotDeletionPolicy DeletionPolicy
		{
			get
			{
			  return sdp;
			}
		}

		/// <summary>
		/// Returns the <seealso cref="IndexWriter"/> used by this <seealso cref="DirectoryTaxonomyWriter"/>. </summary>
		public IndexWriter IndexWriter
		{
			get
			{
			  return writer;
			}
		}

	  }

	  private const int RADIX = 16;

	  public const string INDEX_SOURCE = "index";
	  public const string TAXONOMY_SOURCE = "taxo";

	  private readonly IndexWriter indexWriter;
	  private readonly SnapshotDirectoryTaxonomyWriter taxoWriter;
	  private readonly IndexCommit indexCommit, taxoCommit;
	  private readonly SnapshotDeletionPolicy indexSDP, taxoSDP;
	  private readonly string version;
	  private readonly IDictionary<string, IList<RevisionFile>> sourceFiles;

	  /// <summary>
	  /// Returns a singleton map of the revision files from the given <seealso cref="IndexCommit"/>. </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public static java.util.Map<String, java.util.List<RevisionFile>> revisionFiles(org.apache.lucene.index.IndexCommit indexCommit, org.apache.lucene.index.IndexCommit taxoCommit) throws java.io.IOException
	  public static IDictionary<string, IList<RevisionFile>> revisionFiles(IndexCommit indexCommit, IndexCommit taxoCommit)
	  {
		Dictionary<string, IList<RevisionFile>> files = new Dictionary<string, IList<RevisionFile>>();
		files[INDEX_SOURCE] = IndexRevision.revisionFiles(indexCommit).Values.GetEnumerator().Current;
		files[TAXONOMY_SOURCE] = IndexRevision.revisionFiles(taxoCommit).values().GetEnumerator().next();
		return files;
	  }

	  /// <summary>
	  /// Returns a String representation of a revision's version from the given
	  /// <seealso cref="IndexCommit"/>s of the search and taxonomy indexes.
	  /// </summary>
	  public static string revisionVersion(IndexCommit indexCommit, IndexCommit taxoCommit)
	  {
		return Convert.ToString(indexCommit.Generation, RADIX) + ":" + Convert.ToString(taxoCommit.Generation, RADIX);
	  }

	  /// <summary>
	  /// Constructor over the given <seealso cref="IndexWriter"/>. Uses the last
	  /// <seealso cref="IndexCommit"/> found in the <seealso cref="Directory"/> managed by the given
	  /// writer.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public IndexAndTaxonomyRevision(org.apache.lucene.index.IndexWriter indexWriter, SnapshotDirectoryTaxonomyWriter taxoWriter) throws java.io.IOException
	  public IndexAndTaxonomyRevision(IndexWriter indexWriter, SnapshotDirectoryTaxonomyWriter taxoWriter)
	  {
		IndexDeletionPolicy delPolicy = indexWriter.Config.IndexDeletionPolicy;
		if (!(delPolicy is SnapshotDeletionPolicy))
		{
		  throw new System.ArgumentException("IndexWriter must be created with SnapshotDeletionPolicy");
		}
		this.indexWriter = indexWriter;
		this.taxoWriter = taxoWriter;
		this.indexSDP = (SnapshotDeletionPolicy) delPolicy;
		this.taxoSDP = taxoWriter.DeletionPolicy;
		this.indexCommit = indexSDP.snapshot();
		this.taxoCommit = taxoSDP.snapshot();
		this.version = revisionVersion(indexCommit, taxoCommit);
		this.sourceFiles = revisionFiles(indexCommit, taxoCommit);
	  }

	  public override int compareTo(string version)
	  {
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final String[] parts = version.split(":");
		string[] parts = version.Split(":", true);
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final long indexGen = Long.parseLong(parts[0], RADIX);
		long indexGen = Long.parseLong(parts[0], RADIX);
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final long taxoGen = Long.parseLong(parts[1], RADIX);
		long taxoGen = Long.parseLong(parts[1], RADIX);
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final long indexCommitGen = indexCommit.getGeneration();
		long indexCommitGen = indexCommit.Generation;
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final long taxoCommitGen = taxoCommit.getGeneration();
		long taxoCommitGen = taxoCommit.Generation;

		// if the index generation is not the same as this commit's generation,
		// compare by it. Otherwise, compare by the taxonomy generation.
		if (indexCommitGen < indexGen)
		{
		  return -1;
		}
		else if (indexCommitGen > indexGen)
		{
		  return 1;
		}
		else
		{
		  return taxoCommitGen < taxoGen ? -1 : (taxoCommitGen > taxoGen ? 1 : 0);
		}
	  }

	  public override int compareTo(Revision o)
	  {
		IndexAndTaxonomyRevision other = (IndexAndTaxonomyRevision) o;
		int cmp = indexCommit.compareTo(other.indexCommit);
		return cmp != 0 ? cmp : taxoCommit.compareTo(other.taxoCommit);
	  }

	  public override string Version
	  {
		  get
		  {
			return version;
		  }
	  }

	  public override IDictionary<string, IList<RevisionFile>> SourceFiles
	  {
		  get
		  {
			return sourceFiles;
		  }
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public java.io.InputStream open(String source, String fileName) throws java.io.IOException
	  public override System.IO.Stream open(string source, string fileName)
	  {
		Debug.Assert(source.Equals(INDEX_SOURCE) || source.Equals(TAXONOMY_SOURCE), "invalid source; expected=(" + INDEX_SOURCE + " or " + TAXONOMY_SOURCE + ") got=" + source);
		IndexCommit ic = source.Equals(INDEX_SOURCE) ? indexCommit : taxoCommit;
		return new IndexInputInputStream(ic.Directory.openInput(fileName, IOContext.READONCE));
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public void release() throws java.io.IOException
	  public override void release()
	  {
		try
		{
		  indexSDP.release(indexCommit);
		}
		finally
		{
		  taxoSDP.release(taxoCommit);
		}

		try
		{
		  indexWriter.deleteUnusedFiles();
		}
		finally
		{
		  taxoWriter.IndexWriter.deleteUnusedFiles();
		}
	  }

	  public override string ToString()
	  {
		return "IndexAndTaxonomyRevision version=" + version + " files=" + sourceFiles;
	  }

	}

}