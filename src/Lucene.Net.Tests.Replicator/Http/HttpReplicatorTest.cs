//STATUS: PENDING - 4.8.0

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Replicator;
using Lucene.Net.Replicator.Http;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Lucene.Net.Util;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Directory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Tests.Replicator.Http
{
    public class HttpReplicatorTest : ReplicatorTestCase
    {
        //JAVA:  private File clientWorkDir;
        //JAVA:  private Replicator serverReplicator;
        //JAVA:  private IndexWriter writer;
        //JAVA:  private DirectoryReader reader;
        //JAVA:  private Server server;
        //JAVA:  private int port;
        //JAVA:  private String host;
        //JAVA:  private Directory serverIndexDir, handlerIndexDir;

        private DirectoryInfo clientWorkDir;
        private IReplicator serverReplicator;
        private IndexWriter writer;
        private DirectoryReader reader;

        private int port;
        private string host;
        private TestServer server;

        private Directory serverIndexDir;
        private Directory handlerIndexDir;

        private void StartServer()
        {
            //JAVA:  private void startServer() throws Exception {
            //JAVA:    ServletHandler replicationHandler = new ServletHandler();
            //JAVA:    ReplicationService service = new ReplicationService(Collections.singletonMap("s1", serverReplicator));
            //JAVA:    ServletHolder servlet = new ServletHolder(new ReplicationServlet(service));
            //JAVA:    replicationHandler.addServletWithMapping(servlet, ReplicationService.REPLICATION_CONTEXT + "/*");
            //JAVA:    server = newHttpServer(replicationHandler);
            //JAVA:    port = serverPort(server);
            //JAVA:    host = serverHost(server);
            //JAVA:  }

            ReplicationService service = new ReplicationService(new Dictionary<string, IReplicator> { { "s1", serverReplicator } });

            string baseUrl = "http://localhost:80" + ReplicationService.REPLICATION_CONTEXT;
            server = new TestServer(new WebHostBuilder()
                //.UseUrls(baseUrl)
                .ConfigureServices(container =>
                {
                    container.AddSingleton(service);
                }).UseStartup<ReplicationServlet>());
            server.BaseAddress = new Uri(baseUrl);

            port = ServerPort(server);
            host = ServerHost(server);

            //port = serverPort(server);
            //host = serverHost(server);
        }

        public override void SetUp()
        {
            //JAVA:  public void setUp() throws Exception {
            //JAVA:    super.setUp();
            //JAVA:    System.setProperty("org.eclipse.jetty.LEVEL", "DEBUG"); // sets stderr logging to DEBUG level
            //JAVA:    clientWorkDir = createTempDir("httpReplicatorTest");
            //JAVA:    handlerIndexDir = newDirectory();
            //JAVA:    serverIndexDir = newDirectory();
            //JAVA:    serverReplicator = new LocalReplicator();
            //JAVA:    startServer();
            //JAVA:    
            //JAVA:    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, null);
            //JAVA:    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
            //JAVA:    writer = new IndexWriter(serverIndexDir, conf);
            //JAVA:    reader = DirectoryReader.open(writer, false);
            //JAVA:  }

            base.SetUp();
            //System.setProperty("org.eclipse.jetty.LEVEL", "DEBUG"); // sets stderr logging to DEBUG level
            clientWorkDir = CreateTempDir("httpReplicatorTest");
            handlerIndexDir = NewDirectory();
            serverIndexDir = NewDirectory();
            serverReplicator = new LocalReplicator();
            StartServer();

            IndexWriterConfig conf = NewIndexWriterConfig(TEST_VERSION_CURRENT, null);
            conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
            writer = new IndexWriter(serverIndexDir, conf);
            reader = DirectoryReader.Open(writer, false);
        }

        public override void TearDown()
        {
            //JAVA:  @Override
            //JAVA:  public void tearDown() throws Exception {
            //JAVA:    stopHttpServer(server);
            //JAVA:    IOUtils.close(reader, writer, handlerIndexDir, serverIndexDir);
            //JAVA:    System.clearProperty("org.eclipse.jetty.LEVEL");
            //JAVA:    super.tearDown();
            //JAVA:  }
            StopHttpServer(server);
            IOUtils.Close(reader, writer, handlerIndexDir, serverIndexDir);
            //System.clearProperty("org.eclipse.jetty.LEVEL");
            base.TearDown();
        }

        private void PublishRevision(int id)
        {
            //JAVA:  private void publishRevision(int id) throws IOException {
            //JAVA:    Document doc = new Document();
            //JAVA:    writer.addDocument(doc);
            //JAVA:    writer.setCommitData(Collections.singletonMap("ID", Integer.toString(id, 16)));
            //JAVA:    writer.commit();
            //JAVA:    serverReplicator.publish(new IndexRevision(writer));
            //JAVA:  }
            Document doc = new Document();
            writer.AddDocument(doc);
            writer.SetCommitData(Collections.SingletonMap("ID", id.ToString("X")));
            writer.Commit();
            serverReplicator.Publish(new IndexRevision(writer));
        }

        private void ReopenReader()
        {
            //JAVA:  private void reopenReader() throws IOException {
            //JAVA:    DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
            //JAVA:    assertNotNull(newReader);
            //JAVA:    reader.close();
            //JAVA:    reader = newReader;
            //JAVA:  }
            DirectoryReader newReader = DirectoryReader.OpenIfChanged(reader);
            assertNotNull(newReader);
            reader.Dispose();
            reader = newReader;
        }


        [Test]
        public void TestBasic()
        {
            //JAVA:  public void testBasic() throws Exception {
            //JAVA:    Replicator replicator = new HttpReplicator(host, port, ReplicationService.REPLICATION_CONTEXT + "/s1", 
            //JAVA:        getClientConnectionManager());
            //JAVA:    ReplicationClient client = new ReplicationClient(replicator, new IndexReplicationHandler(handlerIndexDir, null), 
            //JAVA:        new PerSessionDirectoryFactory(clientWorkDir));
            //JAVA:    
            //JAVA:    publishRevision(1);
            //JAVA:    client.updateNow();
            //JAVA:    reopenReader();
            //JAVA:    assertEquals(1, Integer.parseInt(reader.getIndexCommit().getUserData().get("ID"), 16));
            //JAVA:    
            //JAVA:    publishRevision(2);
            //JAVA:    client.updateNow();
            //JAVA:    reopenReader();
            //JAVA:    assertEquals(2, Integer.parseInt(reader.getIndexCommit().getUserData().get("ID"), 16));
            //JAVA:  }


            IReplicator replicator = new HttpReplicator(host, port, ReplicationService.REPLICATION_CONTEXT + "/s1", server.CreateHandler());
            ReplicationClient client = new ReplicationClient(replicator, new IndexReplicationHandler(handlerIndexDir, null), 
                new PerSessionDirectoryFactory(clientWorkDir.FullName));

            PublishRevision(1);
            client.UpdateNow();
            ReopenReader();
            assertEquals(1, int.Parse(reader.IndexCommit.UserData["ID"], NumberStyles.HexNumber));

            PublishRevision(2);
            client.UpdateNow();
            ReopenReader();
            assertEquals(2, int.Parse(reader.IndexCommit.UserData["ID"], NumberStyles.HexNumber));
        }
    }
}
