//STATUS: PENDING - 4.8.0

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Lucene.Net.Tests.Replicator
{
    [TestFixture]
    public class IndexAndTaxonomyReplicationClientTest : ReplicatorTestCase
    {
        //JAVA: private static class IndexAndTaxonomyReadyCallback implements Callable<Boolean>, Closeable {
        //JAVA:   
        //JAVA:   private final Directory indexDir, taxoDir;
        //JAVA:   private DirectoryReader indexReader;
        //JAVA:   private DirectoryTaxonomyReader taxoReader;
        //JAVA:   private FacetsConfig config;
        //JAVA:   private long lastIndexGeneration = -1;
        //JAVA:   
        //JAVA:   public IndexAndTaxonomyReadyCallback(Directory indexDir, Directory taxoDir) throws IOException {
        //JAVA:     this.indexDir = indexDir;
        //JAVA:     this.taxoDir = taxoDir;
        //JAVA:     config = new FacetsConfig();
        //JAVA:     config.setHierarchical("A", true);
        //JAVA:     if (DirectoryReader.indexExists(indexDir)) {
        //JAVA:       indexReader = DirectoryReader.open(indexDir);
        //JAVA:       lastIndexGeneration = indexReader.getIndexCommit().getGeneration();
        //JAVA:       taxoReader = new DirectoryTaxonomyReader(taxoDir);
        //JAVA:     }
        //JAVA:   }
        //JAVA:   
        //JAVA:   @Override
        //JAVA:   public Boolean call() throws Exception {
        //JAVA:     if (indexReader == null) {
        //JAVA:       indexReader = DirectoryReader.open(indexDir);
        //JAVA:       lastIndexGeneration = indexReader.getIndexCommit().getGeneration();
        //JAVA:       taxoReader = new DirectoryTaxonomyReader(taxoDir);
        //JAVA:     } else {
        //JAVA:       // verify search index
        //JAVA:       DirectoryReader newReader = DirectoryReader.openIfChanged(indexReader);
        //JAVA:       assertNotNull("should not have reached here if no changes were made to the index", newReader);
        //JAVA:       long newGeneration = newReader.getIndexCommit().getGeneration();
        //JAVA:       assertTrue("expected newer generation; current=" + lastIndexGeneration + " new=" + newGeneration, newGeneration > lastIndexGeneration);
        //JAVA:       indexReader.close();
        //JAVA:       indexReader = newReader;
        //JAVA:       lastIndexGeneration = newGeneration;
        //JAVA:       TestUtil.checkIndex(indexDir);
        //JAVA:       
        //JAVA:       // verify taxonomy index
        //JAVA:       DirectoryTaxonomyReader newTaxoReader = TaxonomyReader.openIfChanged(taxoReader);
        //JAVA:       if (newTaxoReader != null) {
        //JAVA:         taxoReader.close();
        //JAVA:         taxoReader = newTaxoReader;
        //JAVA:       }
        //JAVA:       TestUtil.checkIndex(taxoDir);
        //JAVA:       
        //JAVA:       // verify faceted search
        //JAVA:       int id = Integer.parseInt(indexReader.getIndexCommit().getUserData().get(VERSION_ID), 16);
        //JAVA:       IndexSearcher searcher = new IndexSearcher(indexReader);
        //JAVA:       FacetsCollector fc = new FacetsCollector();
        //JAVA:       searcher.search(new MatchAllDocsQuery(), fc);
        //JAVA:       Facets facets = new FastTaxonomyFacetCounts(taxoReader, config, fc);
        //JAVA:       assertEquals(1, facets.getSpecificValue("A", Integer.toString(id, 16)).intValue());
        //JAVA:       
        //JAVA:       DrillDownQuery drillDown = new DrillDownQuery(config);
        //JAVA:       drillDown.add("A", Integer.toString(id, 16));
        //JAVA:       TopDocs docs = searcher.search(drillDown, 10);
        //JAVA:       assertEquals(1, docs.totalHits);
        //JAVA:     }
        //JAVA:     return null;
        //JAVA:   }
        //JAVA:   
        //JAVA:   @Override
        //JAVA:   public void close() throws IOException {
        //JAVA:     IOUtils.close(indexReader, taxoReader);
        //JAVA:   }
        //JAVA: }
        //JAVA: 
        //JAVA: private Directory publishIndexDir, publishTaxoDir;
        //JAVA: private MockDirectoryWrapper handlerIndexDir, handlerTaxoDir;
        //JAVA: private Replicator replicator;
        //JAVA: private SourceDirectoryFactory sourceDirFactory;
        //JAVA: private ReplicationClient client;
        //JAVA: private ReplicationHandler handler;
        //JAVA: private IndexWriter publishIndexWriter;
        //JAVA: private SnapshotDirectoryTaxonomyWriter publishTaxoWriter;
        //JAVA: private FacetsConfig config;
        //JAVA: private IndexAndTaxonomyReadyCallback callback;
        //JAVA: private File clientWorkDir;
        //JAVA: 
        //JAVA: private static final String VERSION_ID = "version";
        //JAVA: 
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
        //JAVA: 
        //JAVA: private Revision createRevision(final int id) throws IOException {
        //JAVA:   publishIndexWriter.addDocument(newDocument(publishTaxoWriter, id));
        //JAVA:   publishIndexWriter.setCommitData(new HashMap<String, String>() {{
        //JAVA:     put(VERSION_ID, Integer.toString(id, 16));
        //JAVA:   }});
        //JAVA:   publishIndexWriter.commit();
        //JAVA:   publishTaxoWriter.commit();
        //JAVA:   return new IndexAndTaxonomyRevision(publishIndexWriter, publishTaxoWriter);
        //JAVA: }
        //JAVA: 
        //JAVA: private Document newDocument(TaxonomyWriter taxoWriter, int id) throws IOException {
        //JAVA:   Document doc = new Document();
        //JAVA:   doc.add(new FacetField("A", Integer.toString(id, 16)));
        //JAVA:   return config.build(publishTaxoWriter, doc);
        //JAVA: }
        //JAVA: 
        //JAVA: @Override
        //JAVA: @Before
        //JAVA: public void setUp() throws Exception {
        //JAVA:   super.setUp();
        //JAVA:   publishIndexDir = newDirectory();
        //JAVA:   publishTaxoDir = newDirectory();
        //JAVA:   handlerIndexDir = newMockDirectory();
        //JAVA:   handlerTaxoDir = newMockDirectory();
        //JAVA:   clientWorkDir = createTempDir("replicationClientTest");
        //JAVA:   sourceDirFactory = new PerSessionDirectoryFactory(clientWorkDir);
        //JAVA:   replicator = new LocalReplicator();
        //JAVA:   callback = new IndexAndTaxonomyReadyCallback(handlerIndexDir, handlerTaxoDir);
        //JAVA:   handler = new IndexAndTaxonomyReplicationHandler(handlerIndexDir, handlerTaxoDir, callback);
        //JAVA:   client = new ReplicationClient(replicator, handler, sourceDirFactory);
        //JAVA:   
        //JAVA:   IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, null);
        //JAVA:   conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
        //JAVA:   publishIndexWriter = new IndexWriter(publishIndexDir, conf);
        //JAVA:   publishTaxoWriter = new SnapshotDirectoryTaxonomyWriter(publishTaxoDir);
        //JAVA:   config = new FacetsConfig();
        //JAVA:   config.setHierarchical("A", true);
        //JAVA: }
        //JAVA: 
        //JAVA: @After
        //JAVA: @Override
        //JAVA: public void tearDown() throws Exception {
        //JAVA:   IOUtils.close(client, callback, publishIndexWriter, publishTaxoWriter, replicator, publishIndexDir, publishTaxoDir,
        //JAVA:       handlerIndexDir, handlerTaxoDir);
        //JAVA:   super.tearDown();
        //JAVA: }
        //JAVA: 

        [Test]
        public void TestNoUpdateThread()
        {
            //JAVA: public void testNoUpdateThread() throws Exception {
            //JAVA:   assertNull("no version expected at start", handler.currentVersion());
            //JAVA:   
            //JAVA:   // Callback validates the replicated index
            //JAVA:   replicator.publish(createRevision(1));
            //JAVA:   client.updateNow();
            //JAVA:   
            //JAVA:   // make sure updating twice, when in fact there's nothing to update, works
            //JAVA:   client.updateNow();
            //JAVA:   
            //JAVA:   replicator.publish(createRevision(2));
            //JAVA:   client.updateNow();
            //JAVA:   
            //JAVA:   // Publish two revisions without update, handler should be upgraded to latest
            //JAVA:   replicator.publish(createRevision(3));
            //JAVA:   replicator.publish(createRevision(4));
            //JAVA:   client.updateNow();
            //JAVA: }
            throw new NotImplementedException();
        }

        [Test]
        public void TestRestart()
        {
            //JAVA:   replicator.publish(createRevision(1));
            //JAVA:   client.updateNow();
            //JAVA:   
            //JAVA:   replicator.publish(createRevision(2));
            //JAVA:   client.updateNow();
            //JAVA:   
            //JAVA:   client.stopUpdateThread();
            //JAVA:   client.close();
            //JAVA:   client = new ReplicationClient(replicator, handler, sourceDirFactory);
            //JAVA:   
            //JAVA:   // Publish two revisions without update, handler should be upgraded to latest
            //JAVA:   replicator.publish(createRevision(3));
            //JAVA:   replicator.publish(createRevision(4));
            //JAVA:   client.updateNow();
            throw new NotImplementedException();
        }

        [Test]
        public void TestUpdateThread()
        {
            throw new NotImplementedException();
        }
        //JAVA: 
        //JAVA: @Test
        //JAVA: public void testUpdateThread() throws Exception {
        //JAVA:   client.startUpdateThread(10, "indexTaxo");
        //JAVA:   
        //JAVA:   replicator.publish(createRevision(1));
        //JAVA:   assertHandlerRevision(1, handlerIndexDir);
        //JAVA:   
        //JAVA:   replicator.publish(createRevision(2));
        //JAVA:   assertHandlerRevision(2, handlerIndexDir);
        //JAVA:   
        //JAVA:   // Publish two revisions without update, handler should be upgraded to latest
        //JAVA:   replicator.publish(createRevision(3));
        //JAVA:   replicator.publish(createRevision(4));
        //JAVA:   assertHandlerRevision(4, handlerIndexDir);
        //JAVA: }
        //JAVA: 

        [Test]
        public void TestRecreateTaxonomy()
        {
            throw new NotImplementedException();
        }
        //JAVA: @Test
        //JAVA: public void testRecreateTaxonomy() throws Exception {
        //JAVA:   replicator.publish(createRevision(1));
        //JAVA:   client.updateNow();
        //JAVA:   
        //JAVA:   // recreate index and taxonomy
        //JAVA:   Directory newTaxo = newDirectory();
        //JAVA:   new DirectoryTaxonomyWriter(newTaxo).close();
        //JAVA:   publishTaxoWriter.replaceTaxonomy(newTaxo);
        //JAVA:   publishIndexWriter.deleteAll();
        //JAVA:   replicator.publish(createRevision(2));
        //JAVA:   
        //JAVA:   client.updateNow();
        //JAVA:   newTaxo.close();
        //JAVA: }
        //JAVA:

        [Test]
        public void TestConsistencyOnExceptions()
        {
            throw new NotImplementedException();
        }
        //JAVA: /*
        //JAVA:  * This test verifies that the client and handler do not end up in a corrupt
        //JAVA:  * index if exceptions are thrown at any point during replication. Either when
        //JAVA:  * a client copies files from the server to the temporary space, or when the
        //JAVA:  * handler copies them to the index directory.
        //JAVA:  */
        //JAVA: @Test
        //JAVA: public void testConsistencyOnExceptions() throws Exception {
        //JAVA:   // so the handler's index isn't empty
        //JAVA:   replicator.publish(createRevision(1));
        //JAVA:   client.updateNow();
        //JAVA:   client.close();
        //JAVA:   callback.close();
        //JAVA:
        //JAVA:   // Replicator violates write-once policy. It may be that the
        //JAVA:   // handler copies files to the index dir, then fails to copy a
        //JAVA:   // file and reverts the copy operation. On the next attempt, it
        //JAVA:   // will copy the same file again. There is nothing wrong with this
        //JAVA:   // in a real system, but it does violate write-once, and MDW
        //JAVA:   // doesn't like it. Disabling it means that we won't catch cases
        //JAVA:   // where the handler overwrites an existing index file, but
        //JAVA:   // there's nothing currently we can do about it, unless we don't
        //JAVA:   // use MDW.
        //JAVA:   handlerIndexDir.setPreventDoubleWrite(false);
        //JAVA:   handlerTaxoDir.setPreventDoubleWrite(false);
        //JAVA:
        //JAVA:   // wrap sourceDirFactory to return a MockDirWrapper so we can simulate errors
        //JAVA:   final SourceDirectoryFactory in = sourceDirFactory;
        //JAVA:   final AtomicInteger failures = new AtomicInteger(atLeast(10));
        //JAVA:   sourceDirFactory = new SourceDirectoryFactory() {
        //JAVA:     
        //JAVA:     private long clientMaxSize = 100, handlerIndexMaxSize = 100, handlerTaxoMaxSize = 100;
        //JAVA:     private double clientExRate = 1.0, handlerIndexExRate = 1.0, handlerTaxoExRate = 1.0;
        //JAVA:     
        //JAVA:     @Override
        //JAVA:     public void cleanupSession(String sessionID) throws IOException {
        //JAVA:       in.cleanupSession(sessionID);
        //JAVA:     }
        //JAVA:     
        //JAVA:     @SuppressWarnings("synthetic-access")
        //JAVA:     @Override
        //JAVA:     public Directory getDirectory(String sessionID, String source) throws IOException {
        //JAVA:       Directory dir = in.getDirectory(sessionID, source);
        //JAVA:       if (random().nextBoolean() && failures.get() > 0) { // client should fail, return wrapped dir
        //JAVA:         MockDirectoryWrapper mdw = new MockDirectoryWrapper(random(), dir);
        //JAVA:         mdw.setRandomIOExceptionRateOnOpen(clientExRate);
        //JAVA:         mdw.setMaxSizeInBytes(clientMaxSize);
        //JAVA:         mdw.setRandomIOExceptionRate(clientExRate);
        //JAVA:         mdw.setCheckIndexOnClose(false);
        //JAVA:         clientMaxSize *= 2;
        //JAVA:         clientExRate /= 2;
        //JAVA:         return mdw;
        //JAVA:       }
        //JAVA:       
        //JAVA:       if (failures.get() > 0 && random().nextBoolean()) { // handler should fail
        //JAVA:         if (random().nextBoolean()) { // index dir fail
        //JAVA:           handlerIndexDir.setMaxSizeInBytes(handlerIndexMaxSize);
        //JAVA:           handlerIndexDir.setRandomIOExceptionRate(handlerIndexExRate);
        //JAVA:           handlerIndexDir.setRandomIOExceptionRateOnOpen(handlerIndexExRate);
        //JAVA:           handlerIndexMaxSize *= 2;
        //JAVA:           handlerIndexExRate /= 2;
        //JAVA:         } else { // taxo dir fail
        //JAVA:           handlerTaxoDir.setMaxSizeInBytes(handlerTaxoMaxSize);
        //JAVA:           handlerTaxoDir.setRandomIOExceptionRate(handlerTaxoExRate);
        //JAVA:           handlerTaxoDir.setRandomIOExceptionRateOnOpen(handlerTaxoExRate);
        //JAVA:           handlerTaxoDir.setCheckIndexOnClose(false);
        //JAVA:           handlerTaxoMaxSize *= 2;
        //JAVA:           handlerTaxoExRate /= 2;
        //JAVA:         }
        //JAVA:       } else {
        //JAVA:         // disable all errors
        //JAVA:         handlerIndexDir.setMaxSizeInBytes(0);
        //JAVA:         handlerIndexDir.setRandomIOExceptionRate(0.0);
        //JAVA:         handlerIndexDir.setRandomIOExceptionRateOnOpen(0.0);
        //JAVA:         handlerTaxoDir.setMaxSizeInBytes(0);
        //JAVA:         handlerTaxoDir.setRandomIOExceptionRate(0.0);
        //JAVA:         handlerTaxoDir.setRandomIOExceptionRateOnOpen(0.0);
        //JAVA:       }
        //JAVA:
        //JAVA:       return dir;
        //JAVA:     }
        //JAVA:   };
        //JAVA:   
        //JAVA:   handler = new IndexAndTaxonomyReplicationHandler(handlerIndexDir, handlerTaxoDir, new Callable<Boolean>() {
        //JAVA:     @Override
        //JAVA:     public Boolean call() throws Exception {
        //JAVA:       if (random().nextDouble() < 0.2 && failures.get() > 0) {
        //JAVA:         throw new RuntimeException("random exception from callback");
        //JAVA:       }
        //JAVA:       return null;
        //JAVA:     }
        //JAVA:   });
        //JAVA:
        //JAVA:   // wrap handleUpdateException so we can act on the thrown exception
        //JAVA:   client = new ReplicationClient(replicator, handler, sourceDirFactory) {
        //JAVA:     @SuppressWarnings("synthetic-access")
        //JAVA:     @Override
        //JAVA:     protected void handleUpdateException(Throwable t) {
        //JAVA:       if (t instanceof IOException) {
        //JAVA:         try {
        //JAVA:           if (VERBOSE) {
        //JAVA:             System.out.println("hit exception during update: " + t);
        //JAVA:             t.printStackTrace(System.out);
        //JAVA:           }
        //JAVA:
        //JAVA:           // test that the index can be read and also some basic statistics
        //JAVA:           DirectoryReader reader = DirectoryReader.open(handlerIndexDir.getDelegate());
        //JAVA:           try {
        //JAVA:             int numDocs = reader.numDocs();
        //JAVA:             int version = Integer.parseInt(reader.getIndexCommit().getUserData().get(VERSION_ID), 16);
        //JAVA:             assertEquals(numDocs, version);
        //JAVA:           } finally {
        //JAVA:             reader.close();
        //JAVA:           }
        //JAVA:           // verify index is fully consistent
        //JAVA:           TestUtil.checkIndex(handlerIndexDir.getDelegate());
        //JAVA:           
        //JAVA:           // verify taxonomy index is fully consistent (since we only add one
        //JAVA:           // category to all documents, there's nothing much more to validate
        //JAVA:           TestUtil.checkIndex(handlerTaxoDir.getDelegate());
        //JAVA:         } catch (IOException e) {
        //JAVA:           throw new RuntimeException(e);
        //JAVA:         } finally {
        //JAVA:           // count-down number of failures
        //JAVA:           failures.decrementAndGet();
        //JAVA:           assert failures.get() >= 0 : "handler failed too many times: " + failures.get();
        //JAVA:           if (VERBOSE) {
        //JAVA:             if (failures.get() == 0) {
        //JAVA:               System.out.println("no more failures expected");
        //JAVA:             } else {
        //JAVA:               System.out.println("num failures left: " + failures.get());
        //JAVA:             }
        //JAVA:           }
        //JAVA:         }
        //JAVA:       } else {
        //JAVA:         if (t instanceof RuntimeException) throw (RuntimeException) t;
        //JAVA:         throw new RuntimeException(t);
        //JAVA:       }
        //JAVA:     }
        //JAVA:   };
        //JAVA:   
        //JAVA:   client.startUpdateThread(10, "indexAndTaxo");
        //JAVA:   
        //JAVA:   final Directory baseHandlerIndexDir = handlerIndexDir.getDelegate();
        //JAVA:   int numRevisions = atLeast(20) + 2;
        //JAVA:   for (int i = 2; i < numRevisions; i++) {
        //JAVA:     replicator.publish(createRevision(i));
        //JAVA:     assertHandlerRevision(i, baseHandlerIndexDir);
        //JAVA:   }
        //JAVA:
        //JAVA:   // disable errors -- maybe randomness didn't exhaust all allowed failures,
        //JAVA:   // and we don't want e.g. CheckIndex to hit false errors. 
        //JAVA:   handlerIndexDir.setMaxSizeInBytes(0);
        //JAVA:   handlerIndexDir.setRandomIOExceptionRate(0.0);
        //JAVA:   handlerIndexDir.setRandomIOExceptionRateOnOpen(0.0);
        //JAVA:   handlerTaxoDir.setMaxSizeInBytes(0);
        //JAVA:   handlerTaxoDir.setRandomIOExceptionRate(0.0);
        //JAVA:   handlerTaxoDir.setRandomIOExceptionRateOnOpen(0.0);
        //JAVA: }
        //JAVA:         
    }
}
