using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading;

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


	using AlreadyClosedException = org.apache.lucene.store.AlreadyClosedException;
	using Directory = org.apache.lucene.store.Directory;
	using IOContext = org.apache.lucene.store.IOContext;
	using IndexOutput = org.apache.lucene.store.IndexOutput;
	using IOUtils = org.apache.lucene.util.IOUtils;
	using InfoStream = org.apache.lucene.util.InfoStream;
	using ThreadInterruptedException = org.apache.lucene.util.ThreadInterruptedException;

	/// <summary>
	/// A client which monitors and obtains new revisions from a <seealso cref="Replicator"/>.
	/// It can be used to either periodically check for updates by invoking
	/// <seealso cref="#startUpdateThread"/>, or manually by calling <seealso cref="#updateNow()"/>.
	/// <para>
	/// Whenever a new revision is available, the <seealso cref="#requiredFiles(Map)"/> are
	/// copied to the <seealso cref="Directory"/> specified by <seealso cref="PerSessionDirectoryFactory"/> and
	/// a handler is notified.
	/// 
	/// @lucene.experimental
	/// </para>
	/// </summary>
	public class ReplicationClient : System.IDisposable
	{

	  private class ReplicationThread : System.Threading.Thread
	  {
		  private readonly ReplicationClient outerInstance;


		internal readonly long interval;

		// client uses this to stop us
		internal readonly CountDownLatch stop = new CountDownLatch(1);

		public ReplicationThread(ReplicationClient outerInstance, long interval)
		{
			this.outerInstance = outerInstance;
		  this.interval = interval;
		}

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @SuppressWarnings("synthetic-access") @Override public void run()
		public override void run()
		{
		  while (true)
		  {
			long time = DateTimeHelperClass.CurrentUnixTimeMillis();
			outerInstance.updateLock.@lock();
			try
			{
			  outerInstance.doUpdate();
			}
			catch (Exception t)
			{
			  outerInstance.handleUpdateException(t);
			}
			finally
			{
			  outerInstance.updateLock.unlock();
			}
			time = DateTimeHelperClass.CurrentUnixTimeMillis() - time;

			// adjust timeout to compensate the time spent doing the replication.
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final long timeout = interval - time;
			long timeout = interval - time;
			if (timeout > 0)
			{
			  try
			  {
				// this will return immediately if we were ordered to stop (count=0)
				// or the timeout has elapsed. if it returns true, it means count=0,
				// so terminate.
				if (stop.await(timeout, TimeUnit.MILLISECONDS))
				{
				  return;
				}
			  }
			  catch (InterruptedException e)
			  {
				// if we were interruted, somebody wants to terminate us, so just
				// throw the exception further.
				Thread.CurrentThread.Interrupt();
				throw new ThreadInterruptedException(e);
			  }
			}
		  }
		}

	  }

	  /// <summary>
	  /// Handler for revisions obtained by the client. </summary>
	  public interface ReplicationHandler
	  {

		/// <summary>
		/// Returns the current revision files held by the handler. </summary>
		IDictionary<string, IList<RevisionFile>> currentRevisionFiles();

		/// <summary>
		/// Returns the current revision version held by the handler. </summary>
		string currentVersion();

		/// <summary>
		/// Called when a new revision was obtained and is available (i.e. all needed
		/// files were successfully copied).
		/// </summary>
		/// <param name="version">
		///          the version of the <seealso cref="Revision"/> that was copied </param>
		/// <param name="revisionFiles">
		///          the files contained by this <seealso cref="Revision"/> </param>
		/// <param name="copiedFiles">
		///          the files that were actually copied </param>
		/// <param name="sourceDirectory">
		///          a mapping from a source of files to the <seealso cref="Directory"/> they
		///          were copied into </param>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void revisionReady(String version, java.util.Map<String,java.util.List<RevisionFile>> revisionFiles, java.util.Map<String,java.util.List<String>> copiedFiles, java.util.Map<String, org.apache.lucene.store.Directory> sourceDirectory) throws java.io.IOException;
		void revisionReady(string version, IDictionary<string, IList<RevisionFile>> revisionFiles, IDictionary<string, IList<string>> copiedFiles, IDictionary<string, Directory> sourceDirectory);
	  }

	  /// <summary>
	  /// Resolves a session and source into a <seealso cref="Directory"/> to use for copying
	  /// the session files to.
	  /// </summary>
	  public interface SourceDirectoryFactory
	  {

		/// <summary>
		/// Called to denote that the replication actions for this session were finished and the directory is no longer needed. 
		/// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void cleanupSession(String sessionID) throws java.io.IOException;
		void cleanupSession(string sessionID);

		/// <summary>
		/// Returns the <seealso cref="Directory"/> to use for the given session and source.
		/// Implementations may e.g. return different directories for different
		/// sessions, or the same directory for all sessions. In that case, it is
		/// advised to clean the directory before it is used for a new session.
		/// </summary>
		/// <seealso cref= #cleanupSession(String) </seealso>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public org.apache.lucene.store.Directory getDirectory(String sessionID, String source) throws java.io.IOException;
		Directory getDirectory(string sessionID, string source);

	  }

	  /// <summary>
	  /// The component name to use with <seealso cref="InfoStream#isEnabled(String)"/>. </summary>
	  public const string INFO_STREAM_COMPONENT = "ReplicationThread";

	  private readonly Replicator replicator;
	  private readonly ReplicationHandler handler;
	  private readonly SourceDirectoryFactory factory;
	  private readonly sbyte[] copyBuffer = new sbyte[16384];
	  private readonly Lock updateLock = new ReentrantLock();

	  private volatile ReplicationThread updateThread;
	  private volatile bool closed = false;
	  private volatile InfoStream infoStream = InfoStream.Default;

	  /// <summary>
	  /// Constructor.
	  /// </summary>
	  /// <param name="replicator"> the <seealso cref="Replicator"/> used for checking for updates </param>
	  /// <param name="handler"> notified when new revisions are ready </param>
	  /// <param name="factory"> returns a <seealso cref="Directory"/> for a given source and session  </param>
	  public ReplicationClient(Replicator replicator, ReplicationHandler handler, SourceDirectoryFactory factory)
	  {
		this.replicator = replicator;
		this.handler = handler;
		this.factory = factory;
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private void copyBytes(org.apache.lucene.store.IndexOutput out, java.io.InputStream in) throws java.io.IOException
	  private void copyBytes(IndexOutput @out, System.IO.Stream @in)
	  {
		int numBytes;
		while ((numBytes = @in.Read(copyBuffer, 0, copyBuffer.Length)) > 0)
		{
		  @out.writeBytes(copyBuffer, 0, numBytes);
		}
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private void doUpdate() throws java.io.IOException
	  private void doUpdate()
	  {
		SessionToken session = null;
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final java.util.Map<String,org.apache.lucene.store.Directory> sourceDirectory = new java.util.HashMap<>();
		IDictionary<string, Directory> sourceDirectory = new Dictionary<string, Directory>();
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final java.util.Map<String,java.util.List<String>> copiedFiles = new java.util.HashMap<>();
		IDictionary<string, IList<string>> copiedFiles = new Dictionary<string, IList<string>>();
		bool notify = false;
		try
		{
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final String version = handler.currentVersion();
		  string version = handler.currentVersion();
		  session = replicator.checkForUpdate(version);
		  if (infoStream.isEnabled(INFO_STREAM_COMPONENT))
		  {
			infoStream.message(INFO_STREAM_COMPONENT, "doUpdate(): handlerVersion=" + version + " session=" + session);
		  }
		  if (session == null)
		  {
			// already up to date
			return;
		  }
		  IDictionary<string, IList<RevisionFile>> requiredFiles = requiredFiles(session.sourceFiles);
		  if (infoStream.isEnabled(INFO_STREAM_COMPONENT))
		  {
			infoStream.message(INFO_STREAM_COMPONENT, "doUpdate(): requiredFiles=" + requiredFiles);
		  }
		  foreach (KeyValuePair<string, IList<RevisionFile>> e in requiredFiles.SetOfKeyValuePairs())
		  {
			string source = e.Key;
			Directory dir = factory.getDirectory(session.id, source);
			sourceDirectory[source] = dir;
			IList<string> cpFiles = new List<string>();
			copiedFiles[source] = cpFiles;
			foreach (RevisionFile file in e.Value)
			{
			  if (closed)
			  {
				// if we're closed, abort file copy
				if (infoStream.isEnabled(INFO_STREAM_COMPONENT))
				{
				  infoStream.message(INFO_STREAM_COMPONENT, "doUpdate(): detected client was closed); abort file copy");
				}
				return;
			  }
			  System.IO.Stream @in = null;
			  IndexOutput @out = null;
			  try
			  {
				@in = replicator.obtainFile(session.id, source, file.fileName);
				@out = dir.createOutput(file.fileName, IOContext.DEFAULT);
				copyBytes(@out, @in);
				cpFiles.Add(file.fileName);
				// TODO add some validation, on size / checksum
			  }
			  finally
			  {
				IOUtils.close(@in, @out);
			  }
			}
		  }
		  // only notify if all required files were successfully obtained.
		  notify = true;
		}
		finally
		{
		  if (session != null)
		  {
			try
			{
			  replicator.release(session.id);
			}
			finally
			{
			  if (!notify)
			  { // cleanup after ourselves
				IOUtils.close(sourceDirectory.Values);
				factory.cleanupSession(session.id);
			  }
			}
		  }
		}

		// notify outside the try-finally above, so the session is released sooner.
		// the handler may take time to finish acting on the copied files, but the
		// session itself is no longer needed.
		try
		{
		  if (notify && !closed)
		  { // no use to notify if we are closed already
			handler.revisionReady(session.version, session.sourceFiles, copiedFiles, sourceDirectory);
		  }
		}
		finally
		{
		  IOUtils.close(sourceDirectory.Values);
		  if (session != null)
		  {
			factory.cleanupSession(session.id);
		  }
		}
	  }

	  /// <summary>
	  /// Throws <seealso cref="AlreadyClosedException"/> if the client has already been closed. </summary>
	  protected internal void ensureOpen()
	  {
		if (closed)
		{
		  throw new AlreadyClosedException("this update client has already been closed");
		}
	  }

	  /// <summary>
	  /// Called when an exception is hit by the replication thread. The default
	  /// implementation prints the full stacktrace to the <seealso cref="InfoStream"/> set in
	  /// <seealso cref="#setInfoStream(InfoStream)"/>, or the {@link InfoStream#getDefault()
	  /// default} one. You can override to log the exception elswhere.
	  /// <para>
	  /// <b>NOTE:</b> if you override this method to throw the exception further,
	  /// the replication thread will be terminated. The only way to restart it is to
	  /// call <seealso cref="#stopUpdateThread()"/> followed by
	  /// <seealso cref="#startUpdateThread(long, String)"/>.
	  /// </para>
	  /// </summary>
	  protected internal virtual void handleUpdateException(Exception t)
	  {
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final java.io.StringWriter sw = new java.io.StringWriter();
		StringWriter sw = new StringWriter();
		t.printStackTrace(new PrintWriter(sw));
		if (infoStream.isEnabled(INFO_STREAM_COMPONENT))
		{
		  infoStream.message(INFO_STREAM_COMPONENT, "an error occurred during revision update: " + sw.ToString());
		}
	  }

	  /// <summary>
	  /// Returns the files required for replication. By default, this method returns
	  /// all files that exist in the new revision, but not in the handler.
	  /// </summary>
	  protected internal virtual IDictionary<string, IList<RevisionFile>> requiredFiles(IDictionary<string, IList<RevisionFile>> newRevisionFiles)
	  {
		IDictionary<string, IList<RevisionFile>> handlerRevisionFiles = handler.currentRevisionFiles();
		if (handlerRevisionFiles == null)
		{
		  return newRevisionFiles;
		}

		IDictionary<string, IList<RevisionFile>> requiredFiles = new Dictionary<string, IList<RevisionFile>>();
		foreach (KeyValuePair<string, IList<RevisionFile>> e in handlerRevisionFiles.SetOfKeyValuePairs())
		{
		  // put the handler files in a Set, for faster contains() checks later
		  ISet<string> handlerFiles = new HashSet<string>();
		  foreach (RevisionFile file in e.Value)
		  {
			handlerFiles.Add(file.fileName);
		  }

		  // make sure to preserve revisionFiles order
		  List<RevisionFile> res = new List<RevisionFile>();
		  string source = e.Key;
		  Debug.Assert(newRevisionFiles.ContainsKey(source), "source not found in newRevisionFiles: " + newRevisionFiles);
		  foreach (RevisionFile file in newRevisionFiles[source])
		  {
			if (!handlerFiles.Contains(file.fileName))
			{
			  res.Add(file);
			}
		  }
		  requiredFiles[source] = res;
		}

		return requiredFiles;
	  }

	  public virtual void Dispose()
	  {
		  lock (this)
		  {
			if (!closed)
			{
			  stopUpdateThread();
			  closed = true;
			}
		  }
	  }

	  /// <summary>
	  /// Start the update thread with the specified interval in milliseconds. For
	  /// debugging purposes, you can optionally set the name to set on
	  /// <seealso cref="Thread#setName(String)"/>. If you pass {@code null}, a default name
	  /// will be set.
	  /// </summary>
	  /// <exception cref="IllegalStateException"> if the thread has already been started </exception>
	  public virtual void startUpdateThread(long intervalMillis, string threadName)
	  {
		  lock (this)
		  {
			ensureOpen();
			if (updateThread != null && updateThread.IsAlive)
			{
			  throw new System.InvalidOperationException("cannot start an update thread when one is running, must first call 'stopUpdateThread()'");
			}
			threadName = string.ReferenceEquals(threadName, null) ? INFO_STREAM_COMPONENT : "ReplicationThread-" + threadName;
			updateThread = new ReplicationThread(this, intervalMillis);
			updateThread.Name = threadName;
			updateThread.Start();
			// we rely on isAlive to return true in isUpdateThreadAlive, assert to be on the safe side
			Debug.Assert(updateThread.IsAlive, "updateThread started but not alive?");
		  }
	  }

	  /// <summary>
	  /// Stop the update thread. If the update thread is not running, silently does
	  /// nothing. This method returns after the update thread has stopped.
	  /// </summary>
	  public virtual void stopUpdateThread()
	  {
		  lock (this)
		  {
			if (updateThread != null)
			{
			  // this will trigger the thread to terminate if it awaits the lock.
			  // otherwise, if it's in the middle of replication, we wait for it to
			  // stop.
			  updateThread.stop.countDown();
			  try
			  {
				updateThread.Join();
			  }
			  catch (InterruptedException e)
			  {
				Thread.CurrentThread.Interrupt();
				throw new ThreadInterruptedException(e);
			  }
			  updateThread = null;
			}
		  }
	  }

	  /// <summary>
	  /// Returns true if the update thread is alive. The update thread is alive if
	  /// it has been <seealso cref="#startUpdateThread(long, String) started"/> and not
	  /// <seealso cref="#stopUpdateThread() stopped"/>, as well as didn't hit an error which
	  /// caused it to terminate (i.e. <seealso cref="#handleUpdateException(Throwable)"/>
	  /// threw the exception further).
	  /// </summary>
	  public virtual bool UpdateThreadAlive
	  {
		  get
		  {
			  lock (this)
			  {
				return updateThread != null && updateThread.IsAlive;
			  }
		  }
	  }

	  public override string ToString()
	  {
		string res = "ReplicationClient";
		if (updateThread != null)
		{
		  res += " (" + updateThread.Name + ")";
		}
		return res;
	  }

	  /// <summary>
	  /// Executes the update operation immediately, irregardess if an update thread
	  /// is running or not.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void updateNow() throws java.io.IOException
	  public virtual void updateNow()
	  {
		ensureOpen();
		updateLock.@lock();
		try
		{
		  doUpdate();
		}
		finally
		{
		  updateLock.unlock();
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