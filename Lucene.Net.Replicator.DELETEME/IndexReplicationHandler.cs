using System;
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


	using DirectoryReader = org.apache.lucene.index.DirectoryReader;
	using IndexCommit = org.apache.lucene.index.IndexCommit;
	using IndexFileNames = org.apache.lucene.index.IndexFileNames;
	using IndexNotFoundException = org.apache.lucene.index.IndexNotFoundException;
	using IndexWriter = org.apache.lucene.index.IndexWriter;
	using SegmentInfos = org.apache.lucene.index.SegmentInfos;
	using ReplicationHandler = org.apache.lucene.replicator.ReplicationClient.ReplicationHandler;
	using Directory = org.apache.lucene.store.Directory;
	using IOContext = org.apache.lucene.store.IOContext;
	using InfoStream = org.apache.lucene.util.InfoStream;

	/// <summary>
	/// A <seealso cref="ReplicationHandler"/> for replication of an index. Implements
	/// <seealso cref="#revisionReady"/> by copying the files pointed by the client resolver to
	/// the index <seealso cref="Directory"/> and then touches the index with
	/// <seealso cref="IndexWriter"/> to make sure any unused files are deleted.
	/// <para>
	/// <b>NOTE:</b> this handler assumes that <seealso cref="IndexWriter"/> is not opened by
	/// another process on the index directory. In fact, opening an
	/// <seealso cref="IndexWriter"/> on the same directory to which files are copied can lead
	/// to undefined behavior, where some or all the files will be deleted, override
	/// other files or simply create a mess. When you replicate an index, it is best
	/// if the index is never modified by <seealso cref="IndexWriter"/>, except the one that is
	/// open on the source index, from which you replicate.
	/// </para>
	/// <para>
	/// This handler notifies the application via a provided <seealso cref="Callable"/> when an
	/// updated index commit was made available for it.
	/// 
	/// @lucene.experimental
	/// </para>
	/// </summary>
	public class IndexReplicationHandler : ReplicationHandler
	{

	  /// <summary>
	  /// The component used to log messages to the {@link InfoStream#getDefault()
	  /// default} <seealso cref="InfoStream"/>.
	  /// </summary>
	  public const string INFO_STREAM_COMPONENT = "IndexReplicationHandler";

	  private readonly Directory indexDir;
	  private readonly Callable<bool?> callback;

//JAVA TO C# CONVERTER NOTE: Fields cannot have the same name as methods:
	  private volatile IDictionary<string, IList<RevisionFile>> currentRevisionFiles_Renamed;
//JAVA TO C# CONVERTER NOTE: Fields cannot have the same name as methods:
	  private volatile string currentVersion_Renamed;
	  private volatile InfoStream infoStream = InfoStream.Default;

	  /// <summary>
	  /// Returns the last <seealso cref="IndexCommit"/> found in the <seealso cref="Directory"/>, or
	  /// {@code null} if there are no commits.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public static org.apache.lucene.index.IndexCommit getLastCommit(org.apache.lucene.store.Directory dir) throws java.io.IOException
	  public static IndexCommit getLastCommit(Directory dir)
	  {
		try
		{
		  if (DirectoryReader.indexExists(dir))
		  {
			IList<IndexCommit> commits = DirectoryReader.listCommits(dir);
			// listCommits guarantees that we get at least one commit back, or
			// IndexNotFoundException which we handle below
			return commits[commits.Count - 1];
		  }
		}
		catch (IndexNotFoundException)
		{
		  // ignore the exception and return null
		}
		return null;
	  }

	  /// <summary>
	  /// Verifies that the last file is segments_N and fails otherwise. It also
	  /// removes and returns the file from the list, because it needs to be handled
	  /// last, after all files. This is important in order to guarantee that if a
	  /// reader sees the new segments_N, all other segment files are already on
	  /// stable storage.
	  /// <para>
	  /// The reason why the code fails instead of putting segments_N file last is
	  /// that this indicates an error in the Revision implementation.
	  /// </para>
	  /// </summary>
	  public static string getSegmentsFile(IList<string> files, bool allowEmpty)
	  {
		if (files.Count == 0)
		{
		  if (allowEmpty)
		  {
			return null;
		  }
		  else
		  {
			throw new System.InvalidOperationException("empty list of files not allowed");
		  }
		}

		string segmentsFile = files.Remove(files.Count - 1);
		if (!segmentsFile.StartsWith(IndexFileNames.SEGMENTS, StringComparison.Ordinal) || segmentsFile.Equals(IndexFileNames.SEGMENTS_GEN))
		{
		  throw new System.InvalidOperationException("last file to copy+sync must be segments_N but got " + segmentsFile + "; check your Revision implementation!");
		}
		return segmentsFile;
	  }

	  /// <summary>
	  /// Cleanup the index directory by deleting all given files. Called when file
	  /// copy or sync failed.
	  /// </summary>
	  public static void cleanupFilesOnFailure(Directory dir, IList<string> files)
	  {
		foreach (string file in files)
		{
		  try
		  {
			dir.deleteFile(file);
		  }
		  catch (Exception)
		  {
			// suppress any exception because if we're here, it means copy
			// failed, and we must cleanup after ourselves.
		  }
		}
	  }

	  /// <summary>
	  /// Cleans up the index directory from old index files. This method uses the
	  /// last commit found by <seealso cref="#getLastCommit(Directory)"/>. If it matches the
	  /// expected segmentsFile, then all files not referenced by this commit point
	  /// are deleted.
	  /// <para>
	  /// <b>NOTE:</b> this method does a best effort attempt to clean the index
	  /// directory. It suppresses any exceptions that occur, as this can be retried
	  /// the next time.
	  /// </para>
	  /// </summary>
	  public static void cleanupOldIndexFiles(Directory dir, string segmentsFile)
	  {
		try
		{
		  IndexCommit commit = getLastCommit(dir);
		  // commit == null means weird IO errors occurred, ignore them
		  // if there were any IO errors reading the expected commit point (i.e.
		  // segments files mismatch), then ignore that commit either.
		  if (commit != null && commit.SegmentsFileName.Equals(segmentsFile))
		  {
			ISet<string> commitFiles = new HashSet<string>();
			commitFiles.addAll(commit.FileNames);
			commitFiles.Add(IndexFileNames.SEGMENTS_GEN);
			Matcher matcher = IndexFileNames.CODEC_FILE_PATTERN.matcher("");
			foreach (string file in dir.listAll())
			{
			  if (!commitFiles.Contains(file) && (matcher.reset(file).matches() || file.StartsWith(IndexFileNames.SEGMENTS, StringComparison.Ordinal)))
			  {
				try
				{
				  dir.deleteFile(file);
				}
				catch (Exception)
				{
				  // suppress, it's just a best effort
				}
			  }
			}
		  }
		}
		catch (Exception)
		{
		  // ignore any errors that happens during this state and only log it. this
		  // cleanup will have a chance to succeed the next time we get a new
		  // revision.
		}
	  }

	  /// <summary>
	  /// Copies the files from the source directory to the target one, if they are
	  /// not the same.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public static void copyFiles(org.apache.lucene.store.Directory source, org.apache.lucene.store.Directory target, java.util.List<String> files) throws java.io.IOException
	  public static void copyFiles(Directory source, Directory target, IList<string> files)
	  {
		if (!source.Equals(target))
		{
		  foreach (string file in files)
		  {
			source.copy(target, file, file, IOContext.READONCE);
		  }
		}
	  }

	  /// <summary>
	  /// Writes <seealso cref="IndexFileNames#SEGMENTS_GEN"/> file to the directory, reading
	  /// the generation from the given {@code segmentsFile}. If it is {@code null},
	  /// this method deletes segments.gen from the directory.
	  /// </summary>
	  public static void writeSegmentsGen(string segmentsFile, Directory dir)
	  {
		if (!string.ReferenceEquals(segmentsFile, null))
		{
		  SegmentInfos.writeSegmentsGen(dir, SegmentInfos.generationFromSegmentsFileName(segmentsFile));
		}
		else
		{
		  try
		  {
			dir.deleteFile(IndexFileNames.SEGMENTS_GEN);
		  }
		  catch (Exception)
		  {
			// suppress any errors while deleting this file.
		  }
		}
	  }

	  /// <summary>
	  /// Constructor with the given index directory and callback to notify when the
	  /// indexes were updated.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public IndexReplicationHandler(org.apache.lucene.store.Directory indexDir, java.util.concurrent.Callable<Nullable<bool>> callback) throws java.io.IOException
	  public IndexReplicationHandler(Directory indexDir, Callable<bool?> callback)
	  {
		this.callback = callback;
		this.indexDir = indexDir;
		currentRevisionFiles_Renamed = null;
		currentVersion_Renamed = null;
		if (DirectoryReader.indexExists(indexDir))
		{
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final java.util.List<org.apache.lucene.index.IndexCommit> commits = org.apache.lucene.index.DirectoryReader.listCommits(indexDir);
		  IList<IndexCommit> commits = DirectoryReader.listCommits(indexDir);
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.apache.lucene.index.IndexCommit commit = commits.get(commits.size() - 1);
		  IndexCommit commit = commits[commits.Count - 1];
		  currentRevisionFiles_Renamed = IndexRevision.revisionFiles(commit);
		  currentVersion_Renamed = IndexRevision.revisionVersion(commit);
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.apache.lucene.util.InfoStream infoStream = org.apache.lucene.util.InfoStream.getDefault();
		  InfoStream infoStream = InfoStream.Default;
		  if (infoStream.isEnabled(INFO_STREAM_COMPONENT))
		  {
			infoStream.message(INFO_STREAM_COMPONENT, "constructor(): currentVersion=" + currentVersion_Renamed + " currentRevisionFiles=" + currentRevisionFiles_Renamed);
			infoStream.message(INFO_STREAM_COMPONENT, "constructor(): commit=" + commit);
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
		if (revisionFiles.Count > 1)
		{
		  throw new System.ArgumentException("this handler handles only a single source; got " + revisionFiles.Keys);
		}

		Directory clientDir = sourceDirectory.Values.GetEnumerator().next();
		IList<string> files = copiedFiles.Values.GetEnumerator().next();
		string segmentsFile = getSegmentsFile(files, false);

		bool success = false;
		try
		{
		  // copy files from the client to index directory
		  copyFiles(clientDir, indexDir, files);

		  // fsync all copied files (except segmentsFile)
		  indexDir.sync(files);

		  // now copy and fsync segmentsFile
		  clientDir.copy(indexDir, segmentsFile, segmentsFile, IOContext.READONCE);
		  indexDir.sync(Collections.singletonList(segmentsFile));

		  success = true;
		}
		finally
		{
		  if (!success)
		  {
			files.Add(segmentsFile); // add it back so it gets deleted too
			cleanupFilesOnFailure(indexDir, files);
		  }
		}

		// all files have been successfully copied + sync'd. update the handler's state
		currentRevisionFiles_Renamed = revisionFiles;
		currentVersion_Renamed = version;

		if (infoStream.isEnabled(INFO_STREAM_COMPONENT))
		{
		  infoStream.message(INFO_STREAM_COMPONENT, "revisionReady(): currentVersion=" + currentVersion_Renamed + " currentRevisionFiles=" + currentRevisionFiles_Renamed);
		}

		// update the segments.gen file
		writeSegmentsGen(segmentsFile, indexDir);

		// Cleanup the index directory from old and unused index files.
		// NOTE: we don't use IndexWriter.deleteUnusedFiles here since it may have
		// side-effects, e.g. if it hits sudden IO errors while opening the index
		// (and can end up deleting the entire index). It is not our job to protect
		// against those errors, app will probably hit them elsewhere.
		cleanupOldIndexFiles(indexDir, segmentsFile);

		// successfully updated the index, notify the callback that the index is
		// ready.
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

	  /// <summary>
	  /// Sets the <seealso cref="InfoStream"/> to use for logging messages. </summary>
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