﻿//STATUS: DRAFT - 4.8.0

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Documents;
using Lucene.Net.Facet;
using Lucene.Net.Facet.Taxonomy;
using Lucene.Net.Facet.Taxonomy.Directory;
using Lucene.Net.Index;
using Lucene.Net.Replicator;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Lucene.Net.Util;
using NUnit.Framework;
using Directory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Tests.Replicator
{
    [TestFixture]
    public class IndexAndTaxonomyReplicationClientTest : ReplicatorTestCase
    {
        private class IndexAndTaxonomyReadyCallback : IDisposable
        {
            private Directory indexDir, taxoDir;
            private DirectoryReader indexReader;
            private DirectoryTaxonomyReader taxoReader;
            private FacetsConfig config;
            private long lastIndexGeneration = -1;

            public IndexAndTaxonomyReadyCallback(MockDirectoryWrapper indexDir, MockDirectoryWrapper taxoDir)
            {
                this.indexDir = indexDir;
                this.taxoDir = taxoDir;
                config = new FacetsConfig();
                config.SetHierarchical("A", true);
                if (DirectoryReader.IndexExists(indexDir))
                {
                    indexReader = DirectoryReader.Open(indexDir);
                    lastIndexGeneration = indexReader.IndexCommit.Generation;
                    taxoReader = new DirectoryTaxonomyReader(taxoDir);
                }
            }

            public bool? Call()
            {
                if (indexReader == null)
                {
                    indexReader = DirectoryReader.Open(indexDir);
                    lastIndexGeneration = indexReader.IndexCommit.Generation;
                    taxoReader = new DirectoryTaxonomyReader(taxoDir);
                }
                else
                {
                    // verify search index
                    DirectoryReader newReader = DirectoryReader.OpenIfChanged(indexReader);
                    assertNotNull("should not have reached here if no changes were made to the index", newReader);
                    long newGeneration = newReader.IndexCommit.Generation;
                    assertTrue("expected newer generation; current=" + lastIndexGeneration + " new=" + newGeneration, newGeneration > lastIndexGeneration);
                    indexReader.Dispose();
                    indexReader = newReader;
                    lastIndexGeneration = newGeneration;
                    TestUtil.CheckIndex(indexDir);

                    // verify taxonomy index
                    DirectoryTaxonomyReader newTaxoReader = TaxonomyReader.OpenIfChanged(taxoReader);
                    if (newTaxoReader != null)
                    {
                        taxoReader.Dispose();
                        taxoReader = newTaxoReader;
                    }
                    TestUtil.CheckIndex(taxoDir);

                    // verify faceted search
                    int id = int.Parse(indexReader.IndexCommit.UserData[VERSION_ID], NumberStyles.HexNumber);
                    IndexSearcher searcher = new IndexSearcher(indexReader);
                    FacetsCollector fc = new FacetsCollector();
                    searcher.Search(new MatchAllDocsQuery(), fc);
                    Facets facets = new FastTaxonomyFacetCounts(taxoReader, config, fc);
                    assertEquals(1, (int)facets.GetSpecificValue("A", id.ToString("X")));

                    DrillDownQuery drillDown = new DrillDownQuery(config);
                    drillDown.Add("A", id.ToString("X"));
                    TopDocs docs = searcher.Search(drillDown, 10);
                    assertEquals(1, docs.TotalHits);
                }
                return null;
            }

            public void Dispose()
            {
                IOUtils.Close(indexReader, taxoReader);
            }
        }

        private Directory publishIndexDir, publishTaxoDir;
        private MockDirectoryWrapper handlerIndexDir, handlerTaxoDir;
        private IReplicator replicator;
        private ReplicationClient.ISourceDirectoryFactory sourceDirFactory;
        private ReplicationClient client;
        private ReplicationClient.IReplicationHandler handler;
        private IndexWriter publishIndexWriter;
        private IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter publishTaxoWriter;
        private FacetsConfig config;
        private IndexAndTaxonomyReadyCallback callback;
        private DirectoryInfo clientWorkDir;

        private const string VERSION_ID = "version";

        private void AssertHandlerRevision(int expectedId, Directory dir)
        {
            //JAVA: private void assertHandlerRevision(int expectedID, Directory dir) throws IOException {
            //JAVA:   // loop as long as client is alive. test-framework will terminate us if
            //JAVA:   // there's a serious bug, e.g. client doesn't really update. otherwise,
            //JAVA:   // introducing timeouts is not good, can easily lead to false positives.
            //JAVA:   while (client.isUpdateThreadAlive()) {
            //JAVA:     // give client a chance to update
            //JAVA:     try {
            //JAVA:       Thread.sleep(100);
            //JAVA:     } catch (InterruptedException e) {
            //JAVA:       throw new ThreadInterruptedException(e);
            //JAVA:     }
            //JAVA:     
            //JAVA:     try {
            //JAVA:       DirectoryReader reader = DirectoryReader.open(dir);
            //JAVA:       try {
            //JAVA:         int handlerID = Integer.parseInt(reader.getIndexCommit().getUserData().get(VERSION_ID), 16);
            //JAVA:         if (expectedID == handlerID) {
            //JAVA:           return;
            //JAVA:         }
            //JAVA:       } finally {
            //JAVA:         reader.close();
            //JAVA:       }
            //JAVA:     } catch (Exception e) {
            //JAVA:       // we can hit IndexNotFoundException or e.g. EOFException (on
            //JAVA:       // segments_N) because it is being copied at the same time it is read by
            //JAVA:       // DirectoryReader.open().
            //JAVA:     }
            //JAVA:   }
            //JAVA: }

            // loop as long as client is alive. test-framework will terminate us if
            // there's a serious bug, e.g. client doesn't really update. otherwise,
            // introducing timeouts is not good, can easily lead to false positives.
            while (client.IsUpdateThreadAlive)
            {
                Thread.Sleep(100);

                try
                {
                    DirectoryReader reader = DirectoryReader.Open(dir);
                    try
                    {
                        int handlerId = int.Parse(reader.IndexCommit.UserData[VERSION_ID], NumberStyles.HexNumber);
                        if (expectedId == handlerId)
                        {
                            return;
                        }
                    }
                    finally
                    {
                        reader.Dispose();
                    }
                }
                catch (Exception e)
                {
                    // we can hit IndexNotFoundException or e.g. EOFException (on
                    // segments_N) because it is being copied at the same time it is read by
                    // DirectoryReader.open().
                }
            }
        }

        private IRevision CreateRevision(int id)
        {
            //JAVA: private Revision createRevision(final int id) throws IOException {
            //JAVA:   publishIndexWriter.addDocument(newDocument(publishTaxoWriter, id));
            //JAVA:   publishIndexWriter.setCommitData(new HashMap<String, String>() {{
            //JAVA:     put(VERSION_ID, Integer.toString(id, 16));
            //JAVA:   }});
            //JAVA:   publishIndexWriter.commit();
            //JAVA:   publishTaxoWriter.commit();
            //JAVA:   return new IndexAndTaxonomyRevision(publishIndexWriter, publishTaxoWriter);
            //JAVA: }
            publishIndexWriter.AddDocument(NewDocument(publishTaxoWriter, id));
            publishIndexWriter.SetCommitData(new Dictionary<string, string>{
                { VERSION_ID, id.ToString("X") }
            });
            publishIndexWriter.Commit();
            publishTaxoWriter.Commit();
            return new IndexAndTaxonomyRevision(publishIndexWriter, publishTaxoWriter);
        }

        private Document NewDocument(ITaxonomyWriter taxoWriter, int id)
        {
            Document doc = new Document();
            doc.Add(new FacetField("A", id.ToString("X")));
            return config.Build(taxoWriter, doc);
        }

        public override void SetUp()
        {
            base.SetUp();

            publishIndexDir = NewDirectory();
            publishTaxoDir = NewDirectory();
            handlerIndexDir = NewMockDirectory();
            handlerTaxoDir = NewMockDirectory();
            clientWorkDir = CreateTempDir("replicationClientTest");
            sourceDirFactory = new PerSessionDirectoryFactory(clientWorkDir.FullName);
            replicator = new LocalReplicator();
            callback = new IndexAndTaxonomyReadyCallback(handlerIndexDir, handlerTaxoDir);
            handler = new IndexAndTaxonomyReplicationHandler(handlerIndexDir, handlerTaxoDir, callback.Call);
            client = new ReplicationClient(replicator, handler, sourceDirFactory);

            IndexWriterConfig conf = NewIndexWriterConfig(TEST_VERSION_CURRENT, null);
            conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
            publishIndexWriter = new IndexWriter(publishIndexDir, conf);
            publishTaxoWriter = new IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter(publishTaxoDir);
            config = new FacetsConfig();
            config.SetHierarchical("A", true);
        }

        public override void TearDown()
        {
            IOUtils.Close(client, callback, publishIndexWriter, publishTaxoWriter, replicator, publishIndexDir, publishTaxoDir,
                handlerIndexDir, handlerTaxoDir);
            base.TearDown();
        }

        [Test]
        public void TestNoUpdateThread()
        {
            assertNull("no version expected at start", handler.CurrentVersion);

            // Callback validates the replicated index
            replicator.Publish(CreateRevision(1));
            client.UpdateNow();

            // make sure updating twice, when in fact there's nothing to update, works
            client.UpdateNow();

            replicator.Publish(CreateRevision(2));
            client.UpdateNow();

            // Publish two revisions without update, handler should be upgraded to latest
            replicator.Publish(CreateRevision(3));
            replicator.Publish(CreateRevision(4));
            client.UpdateNow();
        }

        [Test]
        public void TestRestart()
        {
            replicator.Publish(CreateRevision(1));
            client.UpdateNow();

            replicator.Publish(CreateRevision(2));
            client.UpdateNow();

            client.StopUpdateThread();
            client.Dispose();
            client = new ReplicationClient(replicator, handler, sourceDirFactory);

            // Publish two revisions without update, handler should be upgraded to latest
            replicator.Publish(CreateRevision(3));
            replicator.Publish(CreateRevision(4));
            client.UpdateNow();
        }

        [Test]
        public void TestUpdateThread()
        {
            client.StartUpdateThread(10, "indexTaxo");

            replicator.Publish(CreateRevision(1));
            AssertHandlerRevision(1, handlerIndexDir);

            replicator.Publish(CreateRevision(2));
            AssertHandlerRevision(2, handlerIndexDir);

            // Publish two revisions without update, handler should be upgraded to latest
            replicator.Publish(CreateRevision(3));
            replicator.Publish(CreateRevision(4));
            AssertHandlerRevision(4, handlerIndexDir);
        }

        [Test]
        public void TestRecreateTaxonomy()
        {
            replicator.Publish(CreateRevision(1));
            client.UpdateNow();

            // recreate index and taxonomy
            Directory newTaxo = NewDirectory();
            new DirectoryTaxonomyWriter(newTaxo).Dispose();
            publishTaxoWriter.ReplaceTaxonomy(newTaxo);
            publishIndexWriter.DeleteAll();
            replicator.Publish(CreateRevision(2));

            client.UpdateNow();
            newTaxo.Dispose();
        }

        //JAVA: /*
        //JAVA:  * This test verifies that the client and handler do not end up in a corrupt
        //JAVA:  * index if exceptions are thrown at any point during replication. Either when
        //JAVA:  * a client copies files from the server to the temporary space, or when the
        //JAVA:  * handler copies them to the index directory.
        //JAVA:  */
        [Test]
        public void TestConsistencyOnExceptions()
        {
            // so the handler's index isn't empty
            replicator.Publish(CreateRevision(1));
            client.UpdateNow();
            client.Dispose();
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
            handlerIndexDir.PreventDoubleWrite=(false);
            handlerTaxoDir.PreventDoubleWrite = (false);

            // wrap sourceDirFactory to return a MockDirWrapper so we can simulate errors
            ReplicationClient.ISourceDirectoryFactory @in = sourceDirFactory;
            AtomicInt32 failures = new AtomicInt32(AtLeast(10));

            sourceDirFactory = new SourceDirectoryFactoryAnonymousInnerClass(this, @in, failures);
            handler = new IndexAndTaxonomyReplicationHandler(handlerIndexDir, handlerTaxoDir, () =>
            {
                if (Random().NextDouble() < 0.2 && failures.Get() > 0)
                    throw new Exception("random exception from callback");
                return null;
            });
            client = new ReplicationClientAnonymousInnerClass(this, replicator, handler, @in, failures);
            client.StartUpdateThread(10, "indexAndTaxo");

            Directory baseHandlerIndexDir = handlerIndexDir.Delegate;
            int numRevisions = AtLeast(20) + 2;
            for (int i = 2; i < numRevisions; i++)
            {
                replicator.Publish(CreateRevision(i));
                AssertHandlerRevision(i, baseHandlerIndexDir);
            }

            // disable errors -- maybe randomness didn't exhaust all allowed failures,
            // and we don't want e.g. CheckIndex to hit false errors. 
            handlerIndexDir.MaxSizeInBytes=(0);
            handlerIndexDir.RandomIOExceptionRate=(0.0);
            handlerIndexDir.RandomIOExceptionRateOnOpen=(0.0);
            handlerTaxoDir.MaxSizeInBytes=(0);
            handlerTaxoDir.RandomIOExceptionRate=(0.0);
            handlerTaxoDir.RandomIOExceptionRateOnOpen=(0.0);
        }

        private class SourceDirectoryFactoryAnonymousInnerClass : ReplicationClient.ISourceDirectoryFactory
        {
            private long clientMaxSize = 100, handlerIndexMaxSize = 100, handlerTaxoMaxSize = 100;
            private double clientExRate = 1.0, handlerIndexExRate = 1.0, handlerTaxoExRate = 1.0;

            private readonly IndexAndTaxonomyReplicationClientTest test;
            private readonly ReplicationClient.ISourceDirectoryFactory @in;
            private readonly AtomicInt32 failures;

            public SourceDirectoryFactoryAnonymousInnerClass(IndexAndTaxonomyReplicationClientTest test, ReplicationClient.ISourceDirectoryFactory @in, AtomicInt32 failures)
            {
                this.test = test;
                this.@in = @in;
                this.failures = failures;
            }

            public void CleanupSession(string sessionId)
            {
                @in.CleanupSession(sessionId);
            }

            public Directory GetDirectory(string sessionId, string source)
            {
                Directory dir = @in.GetDirectory(sessionId, source);
                if (Random().nextBoolean() && failures.Get() > 0)
                { // client should fail, return wrapped dir
                    MockDirectoryWrapper mdw = new MockDirectoryWrapper(Random(), dir);
                    mdw.RandomIOExceptionRateOnOpen = clientExRate;
                    mdw.MaxSizeInBytes = clientMaxSize;
                    mdw.RandomIOExceptionRate = clientExRate;
                    mdw.CheckIndexOnClose = false;
                    clientMaxSize *= 2;
                    clientExRate /= 2;
                    return mdw;
                }

                if (failures.Get() > 0 && Random().nextBoolean())
                { // handler should fail
                    if (Random().nextBoolean())
                    { // index dir fail
                        test.handlerIndexDir.MaxSizeInBytes=(handlerIndexMaxSize);
                        test.handlerIndexDir.RandomIOExceptionRate = (handlerIndexExRate);
                        test.handlerIndexDir.RandomIOExceptionRateOnOpen = (handlerIndexExRate);
                        handlerIndexMaxSize *= 2;
                        handlerIndexExRate /= 2;
                    }
                    else
                    { // taxo dir fail
                        test.handlerTaxoDir.MaxSizeInBytes = (handlerTaxoMaxSize);
                        test.handlerTaxoDir.RandomIOExceptionRate = (handlerTaxoExRate);
                        test.handlerTaxoDir.RandomIOExceptionRateOnOpen = (handlerTaxoExRate);
                        test.handlerTaxoDir.CheckIndexOnClose = (false);
                        handlerTaxoMaxSize *= 2;
                        handlerTaxoExRate /= 2;
                    }
                }
                else
                {
                    // disable all errors
                    test.handlerIndexDir.MaxSizeInBytes = (0);
                    test.handlerIndexDir.RandomIOExceptionRate = (0.0);
                    test.handlerIndexDir.RandomIOExceptionRateOnOpen = (0.0);
                    test.handlerTaxoDir.MaxSizeInBytes = (0);
                    test.handlerTaxoDir.RandomIOExceptionRate = (0.0);
                    test.handlerTaxoDir.RandomIOExceptionRateOnOpen = (0.0);
                }
                return dir;
            }
        }



        private class ReplicationClientAnonymousInnerClass : ReplicationClient
        {
            private readonly IndexAndTaxonomyReplicationClientTest test;
            private readonly AtomicInt32 failures;

            public ReplicationClientAnonymousInnerClass(IndexAndTaxonomyReplicationClientTest test, IReplicator replicator, IReplicationHandler handler, ISourceDirectoryFactory factory, AtomicInt32 failures)
                : base(replicator, handler, factory)
            {
                this.test = test;
                this.failures = failures;
            }

            protected override void HandleUpdateException(Exception exception)
            {
                if (exception is IOException)
                {
                    try
                    {
                        if (VERBOSE)
                        {
                            Console.WriteLine("hit exception during update: " + exception);
                        }

                        // test that the index can be read and also some basic statistics
                        DirectoryReader reader = DirectoryReader.Open(test.handlerIndexDir.Delegate);
                        try
                        {
                            int numDocs = reader.NumDocs;
                            int version = int.Parse(reader.IndexCommit.UserData[VERSION_ID], NumberStyles.HexNumber);
                            assertEquals(numDocs, version);
                        }
                        finally
                        {
                            reader.Dispose();
                        }
                        // verify index consistency
                        TestUtil.CheckIndex(test.handlerIndexDir.Delegate);

                        // verify taxonomy index is fully consistent (since we only add one
                        // category to all documents, there's nothing much more to validate
                        TestUtil.CheckIndex(test.handlerTaxoDir.Delegate);
                    }
                    //TODO: Java had this, but considering what it does do we need it?
                    //JAVA: catch (IOException e)
                    //JAVA: {
                    //JAVA:     throw new RuntimeException(e);
                    //JAVA: }
                    finally
                    {
                        // count-down number of failures
                        failures.DecrementAndGet();
                        Debug.Assert(failures.Get() >= 0, "handler failed too many times: " + failures.Get());
                        if (VERBOSE)
                        {
                            if (failures.Get() == 0)
                            {
                                Console.WriteLine("no more failures expected");
                            }
                            else
                            {
                                Console.WriteLine("num failures left: " + failures.Get());
                            }
                        }
                    }
                }
                else
                {
                    //JAVA:          if (t instanceof RuntimeException) throw (RuntimeException) t;
                    //JAVA:          throw new RuntimeException(t);
                    throw exception;
                }
            }
        }

    }
}
