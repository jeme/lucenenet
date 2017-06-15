using System;

namespace org.apache.lucene.replicator.http
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
	using Directory = org.apache.lucene.store.Directory;
	using IOUtils = org.apache.lucene.util.IOUtils;
	using TestUtil = org.apache.lucene.util.TestUtil;
	using Server = org.eclipse.jetty.server.Server;
	using ServletHandler = org.eclipse.jetty.servlet.ServletHandler;
	using ServletHolder = org.eclipse.jetty.servlet.ServletHolder;
	using Before = org.junit.Before;
	using Test = org.junit.Test;

	public class HttpReplicatorTest : ReplicatorTestCase
	{

	  private File clientWorkDir;
	  private Replicator serverReplicator;
	  private IndexWriter writer;
	  private DirectoryReader reader;
	  private Server server;
	  private int port;
	  private string host;
	  private Directory serverIndexDir, handlerIndexDir;

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private void startServer() throws Exception
	  private void startServer()
	  {
		ServletHandler replicationHandler = new ServletHandler();
		ReplicationService service = new ReplicationService(Collections.singletonMap("s1", serverReplicator));
		ServletHolder servlet = new ServletHolder(new ReplicationServlet(service));
		replicationHandler.addServletWithMapping(servlet, ReplicationService.REPLICATION_CONTEXT + "/*");
		server = newHttpServer(replicationHandler);
		port = serverPort(server);
		host = serverHost(server);
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Before @Override public void setUp() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public override void setUp()
	  {
		base.setUp();
		System.setProperty("org.eclipse.jetty.LEVEL", "DEBUG"); // sets stderr logging to DEBUG level
		clientWorkDir = createTempDir("httpReplicatorTest");
		handlerIndexDir = newDirectory();
		serverIndexDir = newDirectory();
		serverReplicator = new LocalReplicator();
		startServer();

		IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, null);
		conf.IndexDeletionPolicy = new SnapshotDeletionPolicy(conf.IndexDeletionPolicy);
		writer = new IndexWriter(serverIndexDir, conf);
		reader = DirectoryReader.open(writer, false);
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public void tearDown() throws Exception
	  public override void tearDown()
	  {
		stopHttpServer(server);
		IOUtils.close(reader, writer, handlerIndexDir, serverIndexDir);
		System.clearProperty("org.eclipse.jetty.LEVEL");
		base.tearDown();
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private void publishRevision(int id) throws java.io.IOException
	  private void publishRevision(int id)
	  {
		Document doc = new Document();
		writer.addDocument(doc);
		writer.CommitData = Collections.singletonMap("ID", Convert.ToString(id, 16));
		writer.commit();
		serverReplicator.publish(new IndexRevision(writer));
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private void reopenReader() throws java.io.IOException
	  private void reopenReader()
	  {
		DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
		assertNotNull(newReader);
		reader.close();
		reader = newReader;
	  }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void testBasic() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public virtual void testBasic()
	  {
		Replicator replicator = new HttpReplicator(host, port, ReplicationService.REPLICATION_CONTEXT + "/s1", ClientConnectionManager);
		ReplicationClient client = new ReplicationClient(replicator, new IndexReplicationHandler(handlerIndexDir, null), new PerSessionDirectoryFactory(clientWorkDir));

		publishRevision(1);
		client.updateNow();
		reopenReader();
		assertEquals(1, Convert.ToInt32(reader.IndexCommit.UserData.get("ID"), 16));

		publishRevision(2);
		client.updateNow();
		reopenReader();
		assertEquals(2, Convert.ToInt32(reader.IndexCommit.UserData.get("ID"), 16));
	  }

	}

}