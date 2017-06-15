using System;

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

	using ClientConnectionManager = org.apache.http.conn.ClientConnectionManager;
	using PoolingClientConnectionManager = org.apache.http.impl.conn.PoolingClientConnectionManager;
	using LuceneTestCase = org.apache.lucene.util.LuceneTestCase;
	using SuppressCodecs = org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
	using Connector = org.eclipse.jetty.server.Connector;
	using Handler = org.eclipse.jetty.server.Handler;
	using Server = org.eclipse.jetty.server.Server;
	using SocketConnector = org.eclipse.jetty.server.bio.SocketConnector;
	using SelectChannelConnector = org.eclipse.jetty.server.nio.SelectChannelConnector;
	using HashSessionIdManager = org.eclipse.jetty.server.session.HashSessionIdManager;
	using SslSelectChannelConnector = org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
	using SslSocketConnector = org.eclipse.jetty.server.ssl.SslSocketConnector;
	using SslContextFactory = org.eclipse.jetty.util.ssl.SslContextFactory;
	using QueuedThreadPool = org.eclipse.jetty.util.thread.QueuedThreadPool;
	using AfterClass = org.junit.AfterClass;

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @SuppressCodecs("Lucene3x") public abstract class ReplicatorTestCase extends org.apache.lucene.util.LuceneTestCase
	public abstract class ReplicatorTestCase : LuceneTestCase
	{

	  private static ClientConnectionManager clientConnectionManager;

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @AfterClass public static void afterClassReplicatorTestCase() throws Exception
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
	  public static void afterClassReplicatorTestCase()
	  {
		if (clientConnectionManager != null)
		{
		  clientConnectionManager.shutdown();
		  clientConnectionManager = null;
		}
	  }

	  /// <summary>
	  /// Returns a new <seealso cref="Server HTTP Server"/> instance. To obtain its port, use
	  /// <seealso cref="#serverPort(Server)"/>.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public static synchronized org.eclipse.jetty.server.Server newHttpServer(org.eclipse.jetty.server.Handler handler) throws Exception
	  public static Server newHttpServer(Handler handler)
	  {
		  lock (typeof(ReplicatorTestCase))
		  {
			Server server = new Server(0);
        
			server.Handler = handler;
        
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final String connectorName = System.getProperty("tests.jettyConnector", "SelectChannel");
			string connectorName = System.getProperty("tests.jettyConnector", "SelectChannel");
        
			// if this property is true, then jetty will be configured to use SSL
			// leveraging the same system properties as java to specify
			// the keystore/truststore if they are set
			//
			// This means we will use the same truststore, keystore (and keys) for
			// the server as well as any client actions taken by this JVM in
			// talking to that server, but for the purposes of testing that should 
			// be good enough
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final boolean useSsl = Boolean.getBoolean("tests.jettySsl");
			bool useSsl = Boolean.getBoolean("tests.jettySsl");
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.eclipse.jetty.util.ssl.SslContextFactory sslcontext = new org.eclipse.jetty.util.ssl.SslContextFactory(false);
			SslContextFactory sslcontext = new SslContextFactory(false);
        
			if (useSsl)
			{
			  if (null != System.getProperty("javax.net.ssl.keyStore"))
			  {
				sslcontext.KeyStorePath = System.getProperty("javax.net.ssl.keyStore");
			  }
			  if (null != System.getProperty("javax.net.ssl.keyStorePassword"))
			  {
				sslcontext.KeyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
			  }
			  if (null != System.getProperty("javax.net.ssl.trustStore"))
			  {
				sslcontext.TrustStore = System.getProperty("javax.net.ssl.trustStore");
			  }
			  if (null != System.getProperty("javax.net.ssl.trustStorePassword"))
			  {
				sslcontext.TrustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
			  }
			  sslcontext.NeedClientAuth = Boolean.getBoolean("tests.jettySsl.clientAuth");
			}
        
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.eclipse.jetty.server.Connector connector;
			Connector connector;
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.eclipse.jetty.util.thread.QueuedThreadPool threadPool;
			QueuedThreadPool threadPool;
			if ("SelectChannel".Equals(connectorName))
			{
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.eclipse.jetty.server.nio.SelectChannelConnector c = useSsl ? new org.eclipse.jetty.server.ssl.SslSelectChannelConnector(sslcontext) : new org.eclipse.jetty.server.nio.SelectChannelConnector();
			  SelectChannelConnector c = useSsl ? new SslSelectChannelConnector(sslcontext) : new SelectChannelConnector();
			  c.ReuseAddress = true;
			  c.LowResourcesMaxIdleTime = 1500;
			  connector = c;
			  threadPool = (QueuedThreadPool) c.ThreadPool;
			}
			else if ("Socket".Equals(connectorName))
			{
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.eclipse.jetty.server.bio.SocketConnector c = useSsl ? new org.eclipse.jetty.server.ssl.SslSocketConnector(sslcontext) : new org.eclipse.jetty.server.bio.SocketConnector();
			  SocketConnector c = useSsl ? new SslSocketConnector(sslcontext) : new SocketConnector();
			  c.ReuseAddress = true;
			  connector = c;
			  threadPool = (QueuedThreadPool) c.ThreadPool;
			}
			else
			{
			  throw new System.ArgumentException("Illegal value for system property 'tests.jettyConnector': " + connectorName);
			}
        
			connector.Port = 0;
			connector.Host = "127.0.0.1";
			if (threadPool != null)
			{
			  threadPool.Daemon = true;
			  threadPool.MaxThreads = 10000;
			  threadPool.MaxIdleTimeMs = 5000;
			  threadPool.MaxStopTimeMs = 30000;
			}
        
			server.Connectors = new Connector[] {connector};
			server.SessionIdManager = new HashSessionIdManager(new Random(random().nextLong()));
        
			server.start();
        
			return server;
		  }
	  }

	  /// <summary>
	  /// Returns a <seealso cref="Server"/>'s port. </summary>
	  public static int serverPort(Server server)
	  {
		return server.Connectors[0].LocalPort;
	  }

	  /// <summary>
	  /// Returns a <seealso cref="Server"/>'s host. </summary>
	  public static string serverHost(Server server)
	  {
		return server.Connectors[0].Host;
	  }

	  /// <summary>
	  /// Stops the given HTTP Server instance. This method does its best to guarantee
	  /// that no threads will be left running following this method.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public static void stopHttpServer(org.eclipse.jetty.server.Server httpServer) throws Exception
	  public static void stopHttpServer(Server httpServer)
	  {
		httpServer.stop();
		httpServer.join();
	  }

	  /// <summary>
	  /// Returns a <seealso cref="ClientConnectionManager"/>.
	  /// <para>
	  /// <b>NOTE:</b> do not <seealso cref="ClientConnectionManager#shutdown()"/> this
	  /// connection manager, it will be shutdown automatically after all tests have
	  /// finished.
	  /// </para>
	  /// </summary>
	  public static ClientConnectionManager ClientConnectionManager
	  {
		  get
		  {
			  lock (typeof(ReplicatorTestCase))
			  {
				if (clientConnectionManager == null)
				{
				  PoolingClientConnectionManager ccm = new PoolingClientConnectionManager();
				  ccm.DefaultMaxPerRoute = 128;
				  ccm.MaxTotal = 128;
				  clientConnectionManager = ccm;
				}
            
				return clientConnectionManager;
			  }
		  }
	  }

	}

}