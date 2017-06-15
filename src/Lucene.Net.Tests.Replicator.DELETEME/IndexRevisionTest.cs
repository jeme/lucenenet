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


	using Document = org.apache.lucene.document.Document;
	using IndexFileNames = org.apache.lucene.index.IndexFileNames;
	using IndexWriter = org.apache.lucene.index.IndexWriter;
	using IndexWriterConfig = org.apache.lucene.index.IndexWriterConfig;
	using KeepOnlyLastCommitDeletionPolicy = org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
	using SnapshotDeletionPolicy = org.apache.lucene.index.SnapshotDeletionPolicy;
	using Directory = org.apache.lucene.store.Directory;
	using IOContext = org.apache.lucene.store.IOContext;
	using IndexInput = org.apache.lucene.store.IndexInput;
	using IOUtils = org.apache.lucene.util.IOUtils;
	using Test = org.junit.Test;

	public class IndexRevisionTest : ReplicatorTestCase
	{

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testNoSnapshotDeletionPolicy() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testNoSnapshotDeletionPolicy()
	  {
		Directory dir = newDirectory();
		IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
		conf.IndexDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
		IndexWriter writer = new IndexWriter(dir, conf);
		try
		{
		  assertNotNull(new IndexRevision(writer));
		  fail("should have failed when IndexDeletionPolicy is not Snapshot");
		}
		catch (System.ArgumentException)
		{
		  // expected
		}
		finally
		{
		  IOUtils.close(writer, dir);
		}
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testNoCommit() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testNoCommit()
	  {
		Directory dir = newDirectory();
		IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
		conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
		IndexWriter writer = new IndexWriter(dir, conf);
		try
		{
		  assertNotNull(new IndexRevision(writer));
		  fail("should have failed when there are no commits to snapshot");
		}
		catch (System.InvalidOperationException)
		{
		  // expected
		}
		finally
		{
		  IOUtils.close(writer, dir);
		}
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testRevisionRelease() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testRevisionRelease()
	  {
		Directory dir = newDirectory();
		IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
		conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
		IndexWriter writer = new IndexWriter(dir, conf);
		try
		{
		  writer.addDocument(new Document());
		  writer.commit();
		  Revision rev1 = new IndexRevision(writer);
		  // releasing that revision should not delete the files
		  rev1.release();
		  assertTrue(slowFileExists(dir, IndexFileNames.SEGMENTS + "_1"));

		  rev1 = new IndexRevision(writer); // create revision again, so the files are snapshotted
		  writer.addDocument(new Document());
		  writer.commit();
		  assertNotNull(new IndexRevision(writer));
		  rev1.release(); // this release should trigger the delete of segments_1
		  assertFalse(slowFileExists(dir, IndexFileNames.SEGMENTS + "_1"));
		}
		finally
		{
		  IOUtils.close(writer, dir);
		}
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testSegmentsFileLast() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testSegmentsFileLast()
	  {
		Directory dir = newDirectory();
		IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
		conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
		IndexWriter writer = new IndexWriter(dir, conf);
		try
		{
		  writer.addDocument(new Document());
		  writer.commit();
		  Revision rev = new IndexRevision(writer);
//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @SuppressWarnings("unchecked") java.util.Map<String, java.util.List<RevisionFile>> sourceFiles = rev.getSourceFiles();
		  IDictionary<string, IList<RevisionFile>> sourceFiles = rev.SourceFiles;
		  assertEquals(1, sourceFiles.Count);
		  IList<RevisionFile> files = sourceFiles.Values.GetEnumerator().next();
		  string lastFile = files[files.Count - 1].fileName;
		  assertTrue(lastFile.StartsWith(IndexFileNames.SEGMENTS, StringComparison.Ordinal) && !lastFile.Equals(IndexFileNames.SEGMENTS_GEN));
		}
		finally
		{
		  IOUtils.close(writer, dir);
		}
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testOpen() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testOpen()
	  {
		Directory dir = newDirectory();
		IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
		conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
		IndexWriter writer = new IndexWriter(dir, conf);
		try
		{
		  writer.addDocument(new Document());
		  writer.commit();
		  Revision rev = new IndexRevision(writer);
//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @SuppressWarnings("unchecked") java.util.Map<String, java.util.List<RevisionFile>> sourceFiles = rev.getSourceFiles();
		  IDictionary<string, IList<RevisionFile>> sourceFiles = rev.SourceFiles;
		  string source = sourceFiles.Keys.GetEnumerator().next();
		  foreach (RevisionFile file in sourceFiles.Values.GetEnumerator().next())
		  {
			IndexInput src = dir.openInput(file.fileName, IOContext.READONCE);
			System.IO.Stream @in = rev.open(source, file.fileName);
			assertEquals(src.length(), @in.available());
			sbyte[] srcBytes = new sbyte[(int) src.length()];
			sbyte[] inBytes = new sbyte[(int) src.length()];
			int offset = 0;
			if (random().nextBoolean())
			{
			  int skip = random().Next(10);
			  if (skip >= src.length())
			  {
				skip = 0;
			  }
			  @in.skip(skip);
			  src.seek(skip);
			  offset = skip;
			}
			src.readBytes(srcBytes, offset, srcBytes.Length - offset);
			@in.Read(inBytes, offset, inBytes.Length - offset);
			assertArrayEquals(srcBytes, inBytes);
			IOUtils.close(src, @in);
		  }
		}
		finally
		{
		  IOUtils.close(writer, dir);
		}
	  }

	}

}