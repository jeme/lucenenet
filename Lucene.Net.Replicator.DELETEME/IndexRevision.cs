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


	using IndexCommit = org.apache.lucene.index.IndexCommit;
	using IndexDeletionPolicy = org.apache.lucene.index.IndexDeletionPolicy;
	using IndexWriter = org.apache.lucene.index.IndexWriter;
	using IndexWriterConfig = org.apache.lucene.index.IndexWriterConfig;
	using SnapshotDeletionPolicy = org.apache.lucene.index.SnapshotDeletionPolicy;
	using Directory = org.apache.lucene.store.Directory;
	using IOContext = org.apache.lucene.store.IOContext;

	/// <summary>
	/// A <seealso cref="Revision"/> of a single index files which comprises the list of files
	/// that are part of the current <seealso cref="IndexCommit"/>. To ensure the files are not
	/// deleted by <seealso cref="IndexWriter"/> for as long as this revision stays alive (i.e.
	/// until <seealso cref="#release()"/>), the current commit point is snapshotted, using
	/// <seealso cref="SnapshotDeletionPolicy"/> (this means that the given writer's
	/// <seealso cref="IndexWriterConfig#getIndexDeletionPolicy() config"/> should return
	/// <seealso cref="SnapshotDeletionPolicy"/>).
	/// <para>
	/// When this revision is <seealso cref="#release() released"/>, it releases the obtained
	/// snapshot as well as calls <seealso cref="IndexWriter#deleteUnusedFiles()"/> so that the
	/// snapshotted files are deleted (if they are no longer needed).
	/// 
	/// @lucene.experimental
	/// </para>
	/// </summary>
	public class IndexRevision : Revision
	{

	  private const int RADIX = 16;
	  private const string SOURCE = "index";

	  private readonly IndexWriter writer;
	  private readonly IndexCommit commit;
	  private readonly SnapshotDeletionPolicy sdp;
	  private readonly string version;
	  private readonly IDictionary<string, IList<RevisionFile>> sourceFiles;

	  // returns a RevisionFile with some metadata
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private static RevisionFile newRevisionFile(String file, org.apache.lucene.store.Directory dir) throws java.io.IOException
	  private static RevisionFile newRevisionFile(string file, Directory dir)
	  {
		RevisionFile revFile = new RevisionFile(file);
		revFile.size = dir.fileLength(file);
		return revFile;
	  }

	  /// <summary>
	  /// Returns a singleton map of the revision files from the given <seealso cref="IndexCommit"/>. </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public static java.util.Map<String,java.util.List<RevisionFile>> revisionFiles(org.apache.lucene.index.IndexCommit commit) throws java.io.IOException
	  public static IDictionary<string, IList<RevisionFile>> revisionFiles(IndexCommit commit)
	  {
		ICollection<string> commitFiles = commit.FileNames;
		IList<RevisionFile> revisionFiles = new List<RevisionFile>(commitFiles.Count);
		string segmentsFile = commit.SegmentsFileName;
		Directory dir = commit.Directory;

		foreach (string file in commitFiles)
		{
		  if (!file.Equals(segmentsFile))
		  {
			revisionFiles.Add(newRevisionFile(file, dir));
		  }
		}
		revisionFiles.Add(newRevisionFile(segmentsFile, dir)); // segments_N must be last
		return Collections.singletonMap(SOURCE, revisionFiles);
	  }

	  /// <summary>
	  /// Returns a String representation of a revision's version from the given
	  /// <seealso cref="IndexCommit"/>.
	  /// </summary>
	  public static string revisionVersion(IndexCommit commit)
	  {
		return Convert.ToString(commit.Generation, RADIX);
	  }

	  /// <summary>
	  /// Constructor over the given <seealso cref="IndexWriter"/>. Uses the last
	  /// <seealso cref="IndexCommit"/> found in the <seealso cref="Directory"/> managed by the given
	  /// writer.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public IndexRevision(org.apache.lucene.index.IndexWriter writer) throws java.io.IOException
	  public IndexRevision(IndexWriter writer)
	  {
		IndexDeletionPolicy delPolicy = writer.Config.IndexDeletionPolicy;
		if (!(delPolicy is SnapshotDeletionPolicy))
		{
		  throw new System.ArgumentException("IndexWriter must be created with SnapshotDeletionPolicy");
		}
		this.writer = writer;
		this.sdp = (SnapshotDeletionPolicy) delPolicy;
		this.commit = sdp.snapshot();
		this.version = revisionVersion(commit);
		this.sourceFiles = revisionFiles(commit);
	  }

	  public override int compareTo(string version)
	  {
		long gen = Long.parseLong(version, RADIX);
		long commitGen = commit.Generation;
		return commitGen < gen ? -1 : (commitGen > gen ? 1 : 0);
	  }

	  public override int compareTo(Revision o)
	  {
		IndexRevision other = (IndexRevision) o;
		return commit.compareTo(other.commit);
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
		Debug.Assert(source.Equals(SOURCE), "invalid source; expected=" + SOURCE + " got=" + source);
		return new IndexInputInputStream(commit.Directory.openInput(fileName, IOContext.READONCE));
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public void release() throws java.io.IOException
	  public override void release()
	  {
		sdp.release(commit);
		writer.deleteUnusedFiles();
	  }

	  public override string ToString()
	  {
		return "IndexRevision version=" + version + " files=" + sourceFiles;
	  }

	}

}