using System;
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
	using IndexFileNames = org.apache.lucene.index.IndexFileNames;
	using IndexWriter = org.apache.lucene.index.IndexWriter;
	using IndexWriterConfig = org.apache.lucene.index.IndexWriterConfig;
	using SnapshotDeletionPolicy = org.apache.lucene.index.SnapshotDeletionPolicy;
	using AlreadyClosedException = org.apache.lucene.store.AlreadyClosedException;
	using Directory = org.apache.lucene.store.Directory;
	using IOUtils = org.apache.lucene.util.IOUtils;
	using After = org.junit.After;
	using Before = org.junit.Before;
	using Test = org.junit.Test;

	public class LocalReplicatorTest : ReplicatorTestCase
	{

	  private const string VERSION_ID = "version";

	  private LocalReplicator replicator;
	  private Directory sourceDir;
	  private IndexWriter sourceWriter;

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Before @Override public void setUp() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public override void setUp()
	  {
		base.setUp();
		sourceDir = newDirectory();
		IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, null);
		conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
		sourceWriter = new IndexWriter(sourceDir, conf);
		replicator = new LocalReplicator();
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @After @Override public void tearDown() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public override void tearDown()
	  {
		IOUtils.close(replicator, sourceWriter, sourceDir);
		base.tearDown();
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private Revision createRevision(final int id) throws java.io.IOException
//JAVA TO C# CONVERTER WARNING: 'final' parameters are not available in .NET:
	  private Revision createRevision(int id)
	  {
		sourceWriter.addDocument(new Document());
		sourceWriter.CommitData = new HashMapAnonymousInnerClass(this, id);
		sourceWriter.commit();
		return new IndexRevision(sourceWriter);
	  }

	  private class HashMapAnonymousInnerClass : Dictionary<string, string>
	  {
		  private readonly LocalReplicatorTest outerInstance;

		  private int id;

		  public HashMapAnonymousInnerClass(LocalReplicatorTest outerInstance, int id)
		  {
			  this.outerInstance = outerInstance;
			  this.id = id;

			  this.put(VERSION_ID, Convert.ToString(id, 16));
		  }

	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testCheckForUpdateNoRevisions() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testCheckForUpdateNoRevisions()
	  {
		assertNull(replicator.checkForUpdate(null));
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testObtainFileAlreadyClosed() throws java.io.IOException
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testObtainFileAlreadyClosed()
	  {
		replicator.publish(createRevision(1));
		SessionToken res = replicator.checkForUpdate(null);
		assertNotNull(res);
		assertEquals(1, res.sourceFiles.size());
		KeyValuePair<string, IList<RevisionFile>> entry = res.sourceFiles.entrySet().GetEnumerator().next();
		replicator.close();
		try
		{
		  replicator.obtainFile(res.id, entry.Key, entry.Value.get(0).fileName);
		  fail("should have failed on AlreadyClosedException");
		}
		catch (AlreadyClosedException)
		{
		  // expected
		}
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testPublishAlreadyClosed() throws java.io.IOException
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testPublishAlreadyClosed()
	  {
		replicator.close();
		try
		{
		  replicator.publish(createRevision(2));
		  fail("should have failed on AlreadyClosedException");
		}
		catch (AlreadyClosedException)
		{
		  // expected
		}
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testUpdateAlreadyClosed() throws java.io.IOException
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testUpdateAlreadyClosed()
	  {
		replicator.close();
		try
		{
		  replicator.checkForUpdate(null);
		  fail("should have failed on AlreadyClosedException");
		}
		catch (AlreadyClosedException)
		{
		  // expected
		}
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testPublishSameRevision() throws java.io.IOException
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testPublishSameRevision()
	  {
		Revision rev = createRevision(1);
		replicator.publish(rev);
		SessionToken res = replicator.checkForUpdate(null);
		assertNotNull(res);
		assertEquals(rev.Version, res.version);
		replicator.release(res.id);
		replicator.publish(new IndexRevision(sourceWriter));
		res = replicator.checkForUpdate(res.version);
		assertNull(res);

		// now make sure that publishing same revision doesn't leave revisions
		// "locked", i.e. that replicator releases revisions even when they are not
		// kept
		replicator.publish(createRevision(2));
		assertEquals(1, DirectoryReader.listCommits(sourceDir).size());
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testPublishOlderRev() throws java.io.IOException
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testPublishOlderRev()
	  {
		replicator.publish(createRevision(1));
		Revision old = new IndexRevision(sourceWriter);
		replicator.publish(createRevision(2));
		try
		{
		  replicator.publish(old);
		  fail("should have failed to publish an older revision");
		}
		catch (System.ArgumentException)
		{
		  // expected
		}
		assertEquals(1, DirectoryReader.listCommits(sourceDir).size());
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testObtainMissingFile() throws java.io.IOException
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testObtainMissingFile()
	  {
		replicator.publish(createRevision(1));
		SessionToken res = replicator.checkForUpdate(null);
		try
		{
		  replicator.obtainFile(res.id, res.sourceFiles.Keys.GetEnumerator().next(), "madeUpFile");
		  fail("should have failed obtaining an unrecognized file");
		}
		catch (System.Exception e) when (e is FileNotFoundException || e is NoSuchFileException)
		{
		  // expected
		}
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testSessionExpiration() throws java.io.IOException, InterruptedException
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testSessionExpiration()
	  {
		replicator.publish(createRevision(1));
		SessionToken session = replicator.checkForUpdate(null);
		replicator.ExpirationThreshold = 5; // expire quickly
		Thread.Sleep(50); // sufficient for expiration
		try
		{
		  replicator.obtainFile(session.id, session.sourceFiles.Keys.GetEnumerator().next(), session.sourceFiles.values().GetEnumerator().next().get(0).fileName);
		  fail("should have failed to obtain a file for an expired session");
		}
		catch (SessionExpiredException)
		{
		  // expected
		}
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testUpdateToLatest() throws java.io.IOException
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testUpdateToLatest()
	  {
		replicator.publish(createRevision(1));
		Revision rev = createRevision(2);
		replicator.publish(rev);
		SessionToken res = replicator.checkForUpdate(null);
		assertNotNull(res);
		assertEquals(0, rev.compareTo(res.version));
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testRevisionRelease() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testRevisionRelease()
	  {
		replicator.publish(createRevision(1));
		assertTrue(slowFileExists(sourceDir, IndexFileNames.SEGMENTS + "_1"));
		replicator.publish(createRevision(2));
		// now the files of revision 1 can be deleted
		assertTrue(slowFileExists(sourceDir, IndexFileNames.SEGMENTS + "_2"));
		assertFalse("segments_1 should not be found in index directory after revision is released", slowFileExists(sourceDir, IndexFileNames.SEGMENTS + "_1"));
	  }

	}

}