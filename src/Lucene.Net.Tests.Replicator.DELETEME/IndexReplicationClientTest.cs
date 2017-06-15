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


	using Document = org.apache.lucene.document.Document;
	using DirectoryReader = org.apache.lucene.index.DirectoryReader;
	using IndexWriter = org.apache.lucene.index.IndexWriter;
	using IndexWriterConfig = org.apache.lucene.index.IndexWriterConfig;
	using SnapshotDeletionPolicy = org.apache.lucene.index.SnapshotDeletionPolicy;
	using ReplicationHandler = org.apache.lucene.replicator.ReplicationClient.ReplicationHandler;
	using SourceDirectoryFactory = org.apache.lucene.replicator.ReplicationClient.SourceDirectoryFactory;
	using Directory = org.apache.lucene.store.Directory;
	using MockDirectoryWrapper = org.apache.lucene.store.MockDirectoryWrapper;
	using IOUtils = org.apache.lucene.util.IOUtils;
	using TestUtil = org.apache.lucene.util.TestUtil;
	using ThreadInterruptedException = org.apache.lucene.util.ThreadInterruptedException;
	using After = org.junit.After;
	using Before = org.junit.Before;
	using Test = org.junit.Test;

	public class IndexReplicationClientTest : ReplicatorTestCase
	{

	  private class IndexReadyCallback : Callable<bool?>, System.IDisposable
	  {

		internal readonly Directory indexDir;
		internal DirectoryReader reader;
		internal long lastGeneration = -1;

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public IndexReadyCallback(org.apache.lucene.store.Directory indexDir) throws java.io.IOException
		public IndexReadyCallback(Directory indexDir)
		{
		  this.indexDir = indexDir;
		  if (DirectoryReader.indexExists(indexDir))
		  {
			reader = DirectoryReader.open(indexDir);
			lastGeneration = reader.IndexCommit.Generation;
		  }
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public Nullable<bool> call() throws Exception
		public override bool? call()
		{
		  if (reader == null)
		  {
			reader = DirectoryReader.open(indexDir);
			lastGeneration = reader.IndexCommit.Generation;
		  }
		  else
		  {
			DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
			assertNotNull("should not have reached here if no changes were made to the index", newReader);
			long newGeneration = newReader.IndexCommit.Generation;
			assertTrue("expected newer generation; current=" + lastGeneration + " new=" + newGeneration, newGeneration > lastGeneration);
			reader.close();
			reader = newReader;
			lastGeneration = newGeneration;
			TestUtil.checkIndex(indexDir);
		  }
		  return null;
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public void close() throws java.io.IOException
		public virtual void Dispose()
		{
		  IOUtils.close(reader);
		}
	  }

	  private MockDirectoryWrapper publishDir, handlerDir;
	  private Replicator replicator;
	  private SourceDirectoryFactory sourceDirFactory;
	  private ReplicationClient client;
	  private ReplicationHandler handler;
	  private IndexWriter publishWriter;
	  private IndexReadyCallback callback;

	  private const string VERSION_ID = "version";

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private void assertHandlerRevision(int expectedID, org.apache.lucene.store.Directory dir) throws java.io.IOException
	  private void assertHandlerRevision(int expectedID, Directory dir)
	  {
		// loop as long as client is alive. test-framework will terminate us if
		// there's a serious bug, e.g. client doesn't really update. otherwise,
		// introducing timeouts is not good, can easily lead to false positives.
		while (client.UpdateThreadAlive)
		{
		  // give client a chance to update
		  try
		  {
			Thread.Sleep(100);
		  }
		  catch (InterruptedException e)
		  {
			throw new ThreadInterruptedException(e);
		  }

		  try
		  {
			DirectoryReader reader = DirectoryReader.open(dir);
			try
			{
			  int handlerID = Convert.ToInt32(reader.IndexCommit.UserData.get(VERSION_ID), 16);
			  if (expectedID == handlerID)
			  {
				return;
			  }
			  else if (VERBOSE)
			  {
				Console.WriteLine("expectedID=" + expectedID + " actual=" + handlerID + " generation=" + reader.IndexCommit.Generation);
			  }
			}
			finally
			{
			  reader.close();
			}
		  }
		  catch (Exception)
		  {
			// we can hit IndexNotFoundException or e.g. EOFException (on
			// segments_N) because it is being copied at the same time it is read by
			// DirectoryReader.open().
		  }
		}
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private Revision createRevision(final int id) throws java.io.IOException
//JAVA TO C# CONVERTER WARNING: 'final' parameters are not available in .NET:
	  private Revision createRevision(int id)
	  {
		publishWriter.addDocument(new Document());
		publishWriter.CommitData = new HashMapAnonymousInnerClass(this, id);
		publishWriter.commit();
		return new IndexRevision(publishWriter);
	  }

	  private class HashMapAnonymousInnerClass : Dictionary<string, string>
	  {
		  private readonly IndexReplicationClientTest outerInstance;

		  private int id;

		  public HashMapAnonymousInnerClass(IndexReplicationClientTest outerInstance, int id)
		  {
			  this.outerInstance = outerInstance;
			  this.id = id;

			  this.put(VERSION_ID, Convert.ToString(id, 16));
		  }

	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Override @Before public void setUp() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public override void setUp()
	  {
		base.setUp();
		publishDir = newMockDirectory();
		handlerDir = newMockDirectory();
		sourceDirFactory = new PerSessionDirectoryFactory(createTempDir("replicationClientTest"));
		replicator = new LocalReplicator();
		callback = new IndexReadyCallback(handlerDir);
		handler = new IndexReplicationHandler(handlerDir, callback);
		client = new ReplicationClient(replicator, handler, sourceDirFactory);

		IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, null);
		conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
		publishWriter = new IndexWriter(publishDir, conf);
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @After @Override public void tearDown() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public override void tearDown()
	  {
		IOUtils.close(client, callback, publishWriter, replicator, publishDir, handlerDir);
		base.tearDown();
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testNoUpdateThread() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testNoUpdateThread()
	  {
		assertNull("no version expected at start", handler.currentVersion());

		// Callback validates the replicated index
		replicator.publish(createRevision(1));
		client.updateNow();

		replicator.publish(createRevision(2));
		client.updateNow();

		// Publish two revisions without update, handler should be upgraded to latest
		replicator.publish(createRevision(3));
		replicator.publish(createRevision(4));
		client.updateNow();
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testUpdateThread() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testUpdateThread()
	  {
		client.startUpdateThread(10, "index");

		replicator.publish(createRevision(1));
		assertHandlerRevision(1, handlerDir);

		replicator.publish(createRevision(2));
		assertHandlerRevision(2, handlerDir);

		// Publish two revisions without update, handler should be upgraded to latest
		replicator.publish(createRevision(3));
		replicator.publish(createRevision(4));
		assertHandlerRevision(4, handlerDir);
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testRestart() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testRestart()
	  {
		replicator.publish(createRevision(1));
		client.updateNow();

		replicator.publish(createRevision(2));
		client.updateNow();

		client.stopUpdateThread();
		client.close();
		client = new ReplicationClient(replicator, handler, sourceDirFactory);

		// Publish two revisions without update, handler should be upgraded to latest
		replicator.publish(createRevision(3));
		replicator.publish(createRevision(4));
		client.updateNow();
	  }

	  /*
	   * This test verifies that the client and handler do not end up in a corrupt
	   * index if exceptions are thrown at any point during replication. Either when
	   * a client copies files from the server to the temporary space, or when the
	   * handler copies them to the index directory.
	   */
//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testConsistencyOnExceptions() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testConsistencyOnExceptions()
	  {
		// so the handler's index isn't empty
		replicator.publish(createRevision(1));
		client.updateNow();
		client.close();
		callback.Dispose();

		// Replicator violates write-once policy. It may be that the
		// handler copies files to the index dir, then fails to copy a
		// file and reverts the copy operation. On the next attempt, it
		// will copy the same file again. There is nothing wrong with this
		// in a real system, but it does violate write-once, and MDW
		// doesn't like it. Disabling it means that we won't catch cases
		// where the handler overwrites an existing index file, but
		// there's nothing currently we can do about it, unless we don't
		// use MDW.
		handlerDir.PreventDoubleWrite = false;

		// wrap sourceDirFactory to return a MockDirWrapper so we can simulate errors
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.apache.lucene.replicator.ReplicationClient.SourceDirectoryFactory in = sourceDirFactory;
		SourceDirectoryFactory @in = sourceDirFactory;
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final java.util.concurrent.atomic.AtomicInteger failures = new java.util.concurrent.atomic.AtomicInteger(atLeast(10));
		AtomicInteger failures = new AtomicInteger(atLeast(10));
		sourceDirFactory = new SourceDirectoryFactoryAnonymousInnerClass(this, @in, failures);

		handler = new IndexReplicationHandler(handlerDir, new CallableAnonymousInnerClass(this, failures));

		// wrap handleUpdateException so we can act on the thrown exception
		client = new ReplicationClientAnonymousInnerClass(this, replicator, handler, sourceDirFactory, failures);

		client.startUpdateThread(10, "index");

//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.apache.lucene.store.Directory baseHandlerDir = handlerDir.getDelegate();
		Directory baseHandlerDir = handlerDir.Delegate;
		int numRevisions = atLeast(20);
		for (int i = 2; i < numRevisions; i++)
		{
		  replicator.publish(createRevision(i));
		  assertHandlerRevision(i, baseHandlerDir);
		}

		// disable errors -- maybe randomness didn't exhaust all allowed failures,
		// and we don't want e.g. CheckIndex to hit false errors. 
		handlerDir.MaxSizeInBytes = 0;
		handlerDir.RandomIOExceptionRate = 0.0;
		handlerDir.RandomIOExceptionRateOnOpen = 0.0;
	  }

	  private class SourceDirectoryFactoryAnonymousInnerClass : SourceDirectoryFactory
	  {
		  private readonly IndexReplicationClientTest outerInstance;

		  private SourceDirectoryFactory @in;
		  private AtomicInteger failures;

		  public SourceDirectoryFactoryAnonymousInnerClass(IndexReplicationClientTest outerInstance, SourceDirectoryFactory @in, AtomicInteger failures)
		  {
			  this.outerInstance = outerInstance;
			  this.@in = @in;
			  this.failures = failures;
			  clientMaxSize = 100, handlerMaxSize = 100;
			  clientExRate = 1.0, handlerExRate = 1.0;
		  }


		  private long clientMaxSize;
		  private double clientExRate;

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public void cleanupSession(String sessionID) throws java.io.IOException
		  public override void cleanupSession(string sessionID)
		  {
			@in.cleanupSession(sessionID);
		  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @SuppressWarnings("synthetic-access") @Override public org.apache.lucene.store.Directory getDirectory(String sessionID, String source) throws java.io.IOException
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
		  public override Directory getDirectory(string sessionID, string source)
		  {
			Directory dir = @in.getDirectory(sessionID, source);
			if (random().nextBoolean() && failures.get() > 0)
			{ // client should fail, return wrapped dir
			  MockDirectoryWrapper mdw = new MockDirectoryWrapper(random(), dir);
			  mdw.RandomIOExceptionRateOnOpen = clientExRate;
			  mdw.MaxSizeInBytes = clientMaxSize;
			  mdw.RandomIOExceptionRate = clientExRate;
			  mdw.CheckIndexOnClose = false;
			  clientMaxSize *= 2;
			  clientExRate /= 2;
			  return mdw;
			}

			if (failures.get() > 0 && random().nextBoolean())
			{ // handler should fail
			  outerInstance.handlerDir.MaxSizeInBytes = handlerMaxSize;
			  outerInstance.handlerDir.RandomIOExceptionRateOnOpen = handlerExRate;
			  outerInstance.handlerDir.RandomIOExceptionRate = handlerExRate;
			  handlerMaxSize *= 2;
			  handlerExRate /= 2;
			}
			else
			{
			  // disable errors
			  outerInstance.handlerDir.MaxSizeInBytes = 0;
			  outerInstance.handlerDir.RandomIOExceptionRate = 0.0;
			  outerInstance.handlerDir.RandomIOExceptionRateOnOpen = 0.0;
			}
			return dir;
		  }
	  }

	  private class CallableAnonymousInnerClass : Callable<bool?>
	  {
		  private readonly IndexReplicationClientTest outerInstance;

		  private AtomicInteger failures;

		  public CallableAnonymousInnerClass(IndexReplicationClientTest outerInstance, AtomicInteger failures)
		  {
			  this.outerInstance = outerInstance;
			  this.failures = failures;
		  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public Nullable<bool> call() throws Exception
		  public override bool? call()
		  {
			if (random().NextDouble() < 0.2 && failures.get() > 0)
			{
			  throw new Exception("random exception from callback");
			}
			return null;
		  }
	  }

	  private class ReplicationClientAnonymousInnerClass : ReplicationClient
	  {
		  private readonly IndexReplicationClientTest outerInstance;

		  private AtomicInteger failures;

		  public ReplicationClientAnonymousInnerClass(IndexReplicationClientTest outerInstance, Replicator replicator, ReplicationHandler handler, SourceDirectoryFactory sourceDirFactory, AtomicInteger failures) : base(replicator, handler, sourceDirFactory)
		  {
			  this.outerInstance = outerInstance;
			  this.failures = failures;
		  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @SuppressWarnings("synthetic-access") @Override protected void handleUpdateException(Throwable t)
		  protected internal override void handleUpdateException(Exception t)
		  {
			if (t is IOException)
			{
			  if (VERBOSE)
			  {
				Console.WriteLine("hit exception during update: " + t);
				t.printStackTrace(System.out);
			  }
			  try
			  {
				// test that the index can be read and also some basic statistics
				DirectoryReader reader = DirectoryReader.open(outerInstance.handlerDir.Delegate);
				try
				{
				  int numDocs = reader.numDocs();
				  int version = Convert.ToInt32(reader.IndexCommit.UserData.get(VERSION_ID), 16);
				  assertEquals(numDocs, version);
				}
				finally
				{
				  reader.close();
				}
				// verify index consistency
				TestUtil.checkIndex(outerInstance.handlerDir.Delegate);
			  }
			  catch (IOException e)
			  {
				// exceptions here are bad, don't ignore them
				throw new Exception(e);
			  }
			  finally
			  {
				// count-down number of failures
				failures.decrementAndGet();
				Debug.Assert(failures.get() >= 0, "handler failed too many times: " + failures.get());
				if (VERBOSE)
				{
				  if (failures.get() == 0)
				  {
					Console.WriteLine("no more failures expected");
				  }
				  else
				  {
					Console.WriteLine("num failures left: " + failures.get());
				  }
				}
			  }
			}
			else
			{
			  if (t is Exception)
			  {
				  throw (Exception) t;
			  }
			  throw new Exception(t);
			}
		  }
	  }

	}

}