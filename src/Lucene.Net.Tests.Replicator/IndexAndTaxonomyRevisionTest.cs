//STATUS: DRAFT - 4.8.0

using System;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Facet;
using Lucene.Net.Facet.Taxonomy;
using Lucene.Net.Index;
using Lucene.Net.Replicator;
using Lucene.Net.Store;
using Lucene.Net.Util;
using NUnit.Framework;

namespace Lucene.Net.Tests.Replicator
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

    public class IndexAndTaxonomyRevisionTest : ReplicatorTestCase
    {
        private Document NewDocument(ITaxonomyWriter taxoWriter)
        {
            FacetsConfig config = new FacetsConfig();
            Document doc = new Document();
            doc.Add(new FacetField("A", "1"));
            return config.Build(taxoWriter, doc);
        }

        [Test]
        public void TestNoCommit()
        {
            Directory indexDir = NewDirectory();
            IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
            conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
            IndexWriter indexWriter = new IndexWriter(indexDir, conf);

            Directory taxoDir = NewDirectory();
            IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter taxoWriter = new IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter(taxoDir);
            try
            {
                assertNotNull(new IndexAndTaxonomyRevision(indexWriter, taxoWriter));
                fail("should have failed when there are no commits to snapshot");
            }
            catch (System.InvalidOperationException)
            {
                // expected
            }
            finally
            {
                IOUtils.Close(indexWriter, taxoWriter, taxoDir, indexDir);
            }
        }

        [Test]
        public void TestRevisionRelease()
        {
            Directory indexDir = NewDirectory();
            IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
            conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
            IndexWriter indexWriter = new IndexWriter(indexDir, conf);

            Directory taxoDir = NewDirectory();
            IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter taxoWriter = new IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter(taxoDir);
            try
            {
                indexWriter.AddDocument(NewDocument(taxoWriter));
                indexWriter.Commit();
                taxoWriter.Commit();
                IRevision rev1 = new IndexAndTaxonomyRevision(indexWriter, taxoWriter);
                // releasing that revision should not delete the files
                rev1.Release();
                assertTrue(SlowFileExists(indexDir, IndexFileNames.SEGMENTS + "_1"));
                assertTrue(SlowFileExists(taxoDir, IndexFileNames.SEGMENTS + "_1"));

                rev1 = new IndexAndTaxonomyRevision(indexWriter, taxoWriter); // create revision again, so the files are snapshotted
                indexWriter.AddDocument(NewDocument(taxoWriter));
                indexWriter.Commit();
                taxoWriter.Commit();
                assertNotNull(new IndexAndTaxonomyRevision(indexWriter, taxoWriter));
                rev1.Release(); // this release should trigger the delete of segments_1
                assertFalse(SlowFileExists(indexDir, IndexFileNames.SEGMENTS + "_1"));
            }
            finally
            {
                IOUtils.Close(indexWriter, taxoWriter, taxoDir, indexDir);
            }
        }

        [Test]
        public void TestSegmentsFileLast()
        {
            Directory indexDir = NewDirectory();
            IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
            conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
            IndexWriter indexWriter = new IndexWriter(indexDir, conf);

            Directory taxoDir = NewDirectory();
            IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter taxoWriter = new IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter(taxoDir);
            try
            {
                indexWriter.AddDocument(NewDocument(taxoWriter));
                indexWriter.Commit();
                taxoWriter.Commit();
                IRevision rev = new IndexAndTaxonomyRevision(indexWriter, taxoWriter);
                var sourceFiles = rev.SourceFiles;
                assertEquals(2, sourceFiles.Count);
                foreach (var files in sourceFiles.Values)
                {
                    string lastFile = files.Last().FileName;
                    assertTrue(lastFile.StartsWith(IndexFileNames.SEGMENTS, StringComparison.Ordinal) && !lastFile.Equals(IndexFileNames.SEGMENTS_GEN));
                }
            }
            finally
            {
                IOUtils.Close(indexWriter, taxoWriter, taxoDir, indexDir);
            }
        }

        [Test]
        public void TestOpen()
        {
            Directory indexDir = NewDirectory();
            IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
            conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
            IndexWriter indexWriter = new IndexWriter(indexDir, conf);

            Directory taxoDir = NewDirectory();
            IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter taxoWriter = new IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter(taxoDir);
            try
            {
                indexWriter.AddDocument(NewDocument(taxoWriter));
                indexWriter.Commit();
                taxoWriter.Commit();
                IRevision rev = new IndexAndTaxonomyRevision(indexWriter, taxoWriter);
                foreach (var e in rev.SourceFiles)
                {
                    string source = e.Key;
                    Directory dir = source.Equals(IndexAndTaxonomyRevision.INDEX_SOURCE) ? indexDir : taxoDir;
                    foreach (RevisionFile file in e.Value)
                    {
                        IndexInput src = dir.OpenInput(file.FileName, IOContext.READ_ONCE);
                        System.IO.Stream @in = rev.Open(source, file.FileName);
                        assertEquals(src.Length, @in.Length);
                        byte[] srcBytes = new byte[(int)src.Length];
                        byte[] inBytes = new byte[(int)src.Length];
                        int offset = 0;
                        if (Random().nextBoolean())
                        {
                            int skip = Random().Next(10);
                            if (skip >= src.Length)
                            {
                                skip = 0;
                            }
                            //JAVA: in.skip(skip);
                            byte[] skips = new byte[skip];
                            @in.Read(skips, 0, skip);
                            src.Seek(skip);
                            offset = skip;
                        }
                        src.ReadBytes(srcBytes, offset, srcBytes.Length - offset);
                        @in.Read(inBytes, offset, inBytes.Length - offset);
                        assertArrayEquals(srcBytes, inBytes);
                        IOUtils.Close(src, @in);
                    }
                }
            }
            finally
            {
                IOUtils.Close(indexWriter, taxoWriter, taxoDir, indexDir);
            }
        }
    }
}