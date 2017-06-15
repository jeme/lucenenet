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


	using HttpResponse = org.apache.http.HttpResponse;
	using ClientConnectionManager = org.apache.http.conn.ClientConnectionManager;
	using ReplicationAction = org.apache.lucene.replicator.http.ReplicationService.ReplicationAction;

	/// <summary>
	/// An HTTP implementation of <seealso cref="Replicator"/>. Assumes the API supported by
	/// <seealso cref="ReplicationService"/>.
	/// 
	/// @lucene.experimental
	/// </summary>
	public class HttpReplicator : HttpClientBase, Replicator
	{

	  /// <summary>
	  /// Construct with specified connection manager. </summary>
	  public HttpReplicator(string host, int port, string path, ClientConnectionManager conMgr) : base(host, port, path, conMgr)
	  {
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public org.apache.lucene.replicator.SessionToken checkForUpdate(String currVersion) throws java.io.IOException
	  public override SessionToken checkForUpdate(string currVersion)
	  {
		string[] @params = null;
		if (!string.ReferenceEquals(currVersion, null))
		{
		  @params = new string[] {ReplicationService.REPLICATE_VERSION_PARAM, currVersion};
		}
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.apache.http.HttpResponse response = executeGET(org.apache.lucene.replicator.http.ReplicationService.ReplicationAction.UPDATE.name(), params);
		HttpResponse response = executeGET(ReplicationAction.UPDATE.name(), @params);
		return doAction(response, new CallableAnonymousInnerClass(this, response));
	  }

	  private class CallableAnonymousInnerClass : Callable<SessionToken>
	  {
		  private readonly HttpReplicator outerInstance;

		  private HttpResponse response;

		  public CallableAnonymousInnerClass(HttpReplicator outerInstance, HttpResponse response)
		  {
			  this.outerInstance = outerInstance;
			  this.response = response;
		  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public org.apache.lucene.replicator.SessionToken call() throws Exception
		  public override SessionToken call()
		  {
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final java.io.DataInputStream dis = new java.io.DataInputStream(responseInputStream(response));
			DataInputStream dis = new DataInputStream(outerInstance.responseInputStream(response));
			try
			{
			  if (dis.readByte() == 0)
			  {
				return null;
			  }
			  else
			  {
				return new SessionToken(dis);
			  }
			}
			finally
			{
			  dis.close();
			}
		  }
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public java.io.InputStream obtainFile(String sessionID, String source, String fileName) throws java.io.IOException
	  public override System.IO.Stream obtainFile(string sessionID, string source, string fileName)
	  {
		string[] @params = new string[] {ReplicationService.REPLICATE_SESSION_ID_PARAM, sessionID, ReplicationService.REPLICATE_SOURCE_PARAM, source, ReplicationService.REPLICATE_FILENAME_PARAM, fileName};
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.apache.http.HttpResponse response = executeGET(org.apache.lucene.replicator.http.ReplicationService.ReplicationAction.OBTAIN.name(), params);
		HttpResponse response = executeGET(ReplicationAction.OBTAIN.name(), @params);
		return doAction(response, false, new CallableAnonymousInnerClass2(this, response));
	  }

	  private class CallableAnonymousInnerClass2 : Callable<System.IO.Stream>
	  {
		  private readonly HttpReplicator outerInstance;

		  private HttpResponse response;

		  public CallableAnonymousInnerClass2(HttpReplicator outerInstance, HttpResponse response)
		  {
			  this.outerInstance = outerInstance;
			  this.response = response;
		  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public java.io.InputStream call() throws Exception
		  public override System.IO.Stream call()
		  {
			return outerInstance.responseInputStream(response,true);
		  }
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public void publish(org.apache.lucene.replicator.Revision revision) throws java.io.IOException
	  public override void publish(Revision revision)
	  {
		throw new System.NotSupportedException("this replicator implementation does not support remote publishing of revisions");
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public void release(String sessionID) throws java.io.IOException
	  public override void release(string sessionID)
	  {
		string[] @params = new string[] {ReplicationService.REPLICATE_SESSION_ID_PARAM, sessionID};
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.apache.http.HttpResponse response = executeGET(org.apache.lucene.replicator.http.ReplicationService.ReplicationAction.RELEASE.name(), params);
		HttpResponse response = executeGET(ReplicationAction.RELEASE.name(), @params);
		doAction(response, new CallableAnonymousInnerClass3(this));
	  }

	  private class CallableAnonymousInnerClass3 : Callable<object>
	  {
		  private readonly HttpReplicator outerInstance;

		  public CallableAnonymousInnerClass3(HttpReplicator outerInstance)
		  {
			  this.outerInstance = outerInstance;
		  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public Object call() throws Exception
		  public override object call()
		  {
			return null; // do not remove this call: as it is still validating for us!
		  }
	  }

	}

}