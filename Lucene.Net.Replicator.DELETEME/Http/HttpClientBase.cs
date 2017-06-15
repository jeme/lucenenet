using System;
using System.Text;

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


	using HttpEntity = org.apache.http.HttpEntity;
	using HttpResponse = org.apache.http.HttpResponse;
	using HttpStatus = org.apache.http.HttpStatus;
	using StatusLine = org.apache.http.StatusLine;
	using HttpClient = org.apache.http.client.HttpClient;
	using HttpGet = org.apache.http.client.methods.HttpGet;
	using HttpPost = org.apache.http.client.methods.HttpPost;
	using ClientConnectionManager = org.apache.http.conn.ClientConnectionManager;
	using DefaultHttpClient = org.apache.http.impl.client.DefaultHttpClient;
	using HttpConnectionParams = org.apache.http.@params.HttpConnectionParams;
	using EntityUtils = org.apache.http.util.EntityUtils;
	using AlreadyClosedException = org.apache.lucene.store.AlreadyClosedException;

	/// <summary>
	/// Base class for Http clients.
	/// 
	/// @lucene.experimental
	/// 
	/// </summary>
	public abstract class HttpClientBase : System.IDisposable
	{

	  /// <summary>
	  /// Default connection timeout for this client, in milliseconds.
	  /// </summary>
	  /// <seealso cref= #setConnectionTimeout(int) </seealso>
	  public const int DEFAULT_CONNECTION_TIMEOUT = 1000;

	  /// <summary>
	  /// Default socket timeout for this client, in milliseconds.
	  /// </summary>
	  /// <seealso cref= #setSoTimeout(int) </seealso>
	  public const int DEFAULT_SO_TIMEOUT = 60000;

	  // TODO compression?

	  /// <summary>
	  /// The URL stting to execute requests against. </summary>
	  protected internal readonly string url;

	  private volatile bool closed = false;

	  private readonly HttpClient httpc;

	  /// <param name="conMgr"> connection manager to use for this http client.
	  ///        <b>NOTE:</b>The provided <seealso cref="ClientConnectionManager"/> will not be
	  ///        <seealso cref="ClientConnectionManager#shutdown()"/> by this class. </param>
	  protected internal HttpClientBase(string host, int port, string path, ClientConnectionManager conMgr)
	  {
		url = normalizedURL(host, port, path);
		httpc = new DefaultHttpClient(conMgr);
		ConnectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
		SoTimeout = DEFAULT_SO_TIMEOUT;
	  }

	  /// <summary>
	  /// Set the connection timeout for this client, in milliseconds. This setting
	  /// is used to modify <seealso cref="HttpConnectionParams#setConnectionTimeout"/>.
	  /// </summary>
	  /// <param name="timeout"> timeout to set, in millisecopnds </param>
	  public virtual int ConnectionTimeout
	  {
		  set
		  {
			HttpConnectionParams.setConnectionTimeout(httpc.Params, value);
		  }
	  }

	  /// <summary>
	  /// Set the socket timeout for this client, in milliseconds. This setting
	  /// is used to modify <seealso cref="HttpConnectionParams#setSoTimeout"/>.
	  /// </summary>
	  /// <param name="timeout"> timeout to set, in millisecopnds </param>
	  public virtual int SoTimeout
	  {
		  set
		  {
			HttpConnectionParams.setSoTimeout(httpc.Params, value);
		  }
	  }

	  /// <summary>
	  /// Throws <seealso cref="AlreadyClosedException"/> if this client is already closed. </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: protected final void ensureOpen() throws org.apache.lucene.store.AlreadyClosedException
	  protected internal void ensureOpen()
	  {
		if (closed)
		{
		  throw new AlreadyClosedException("HttpClient already closed");
		}
	  }

	  /// <summary>
	  /// Create a URL out of the given parameters, translate an empty/null path to '/'
	  /// </summary>
	  private static string normalizedURL(string host, int port, string path)
	  {
		if (string.ReferenceEquals(path, null) || path.Length == 0)
		{
		  path = "/";
		}
		return "http://" + host + ":" + port + path;
	  }

	  /// <summary>
	  /// <b>Internal:</b> response status after invocation, and in case or error attempt to read the 
	  /// exception sent by the server. 
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: protected void verifyStatus(org.apache.http.HttpResponse response) throws java.io.IOException
	  protected internal virtual void verifyStatus(HttpResponse response)
	  {
		StatusLine statusLine = response.StatusLine;
		if (statusLine.StatusCode != HttpStatus.SC_OK)
		{
		  throwKnownError(response, statusLine);
		}
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: protected void throwKnownError(org.apache.http.HttpResponse response, org.apache.http.StatusLine statusLine) throws java.io.IOException
	  protected internal virtual void throwKnownError(HttpResponse response, StatusLine statusLine)
	  {
		ObjectInputStream @in = null;
		try
		{
		  @in = new ObjectInputStream(response.Entity.Content);
		}
		catch (Exception)
		{
		  // the response stream is not an exception - could be an error in servlet.init().
		  throw new Exception("Uknown error: " + statusLine);
		}

		Exception t;
		try
		{
		  t = (Exception) @in.readObject();
		}
		catch (Exception e)
		{
		  //not likely
		  throw new Exception("Failed to read exception object: " + statusLine, e);
		}
		finally
		{
		  @in.close();
		}
		if (t is IOException)
		{
		  throw (IOException) t;
		}
		if (t is Exception)
		{
		  throw (Exception) t;
		}
		throw new Exception("unknown exception " + statusLine,t);
	  }

	  /// <summary>
	  /// <b>internal:</b> execute a request and return its result
	  /// The <code>params</code> argument is treated as: name1,value1,name2,value2,...
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: protected org.apache.http.HttpResponse executePOST(String request, org.apache.http.HttpEntity entity, String... params) throws java.io.IOException
	  protected internal virtual HttpResponse executePOST(string request, HttpEntity entity, params string[] @params)
	  {
		ensureOpen();
		HttpPost m = new HttpPost(queryString(request, @params));
		m.Entity = entity;
		HttpResponse response = httpc.execute(m);
		verifyStatus(response);
		return response;
	  }

	  /// <summary>
	  /// <b>internal:</b> execute a request and return its result
	  /// The <code>params</code> argument is treated as: name1,value1,name2,value2,...
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: protected org.apache.http.HttpResponse executeGET(String request, String... params) throws java.io.IOException
	  protected internal virtual HttpResponse executeGET(string request, params string[] @params)
	  {
		ensureOpen();
		HttpGet m = new HttpGet(queryString(request, @params));
		HttpResponse response = httpc.execute(m);
		verifyStatus(response);
		return response;
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private String queryString(String request, String... params) throws java.io.UnsupportedEncodingException
	  private string queryString(string request, params string[] @params)
	  {
		StringBuilder query = (new StringBuilder(url)).Append('/').Append(request).Append('?');
		if (@params != null)
		{
		  for (int i = 0; i < @params.Length; i += 2)
		  {
			query.Append(@params[i]).Append('=').Append(URLEncoder.encode(@params[i + 1], "UTF8")).Append('&');
		  }
		}
		return query.substring(0, query.Length - 1);
	  }

	  /// <summary>
	  /// Internal utility: input stream of the provided response </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public java.io.InputStream responseInputStream(org.apache.http.HttpResponse response) throws java.io.IOException
	  public virtual System.IO.Stream responseInputStream(HttpResponse response)
	  {
		return responseInputStream(response, false);
	  }

	  // TODO: can we simplify this Consuming !?!?!?
	  /// <summary>
	  /// Internal utility: input stream of the provided response, which optionally 
	  /// consumes the response's resources when the input stream is exhausted.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public java.io.InputStream responseInputStream(org.apache.http.HttpResponse response, boolean consume) throws java.io.IOException
	  public virtual System.IO.Stream responseInputStream(HttpResponse response, bool consume)
	  {
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.apache.http.HttpEntity entity = response.getEntity();
		HttpEntity entity = response.Entity;
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final java.io.InputStream in = entity.getContent();
		System.IO.Stream @in = entity.Content;
		if (!consume)
		{
		  return @in;
		}
		return new InputStreamAnonymousInnerClass(this, entity, @in);
	  }

	  private class InputStreamAnonymousInnerClass : System.IO.Stream
	  {
		  private readonly HttpClientBase outerInstance;

		  private HttpEntity entity;
		  private System.IO.Stream @in;

		  public InputStreamAnonymousInnerClass(HttpClientBase outerInstance, HttpEntity entity, System.IO.Stream @in)
		  {
			  this.outerInstance = outerInstance;
			  this.entity = entity;
			  this.@in = @in;
			  consumed = false;
		  }

		  private bool consumed;
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public int read() throws java.io.IOException
		  public override int read()
		  {
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final int res = in.read();
			int res = @in.Read();
			consume(res);
			return res;
		  }
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public void close() throws java.io.IOException
		  public override void close()
		  {
			base.close();
			consume(-1);
		  }
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public int read(byte[] b) throws java.io.IOException
		  public override int read(sbyte[] b)
		  {
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final int res = super.read(b);
			int res = base.read(b);
			consume(res);
			return res;
		  }
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public int read(byte[] b, int off, int len) throws java.io.IOException
		  public override int read(sbyte[] b, int off, int len)
		  {
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final int res = super.read(b, off, len);
			int res = base.read(b, off, len);
			consume(res);
			return res;
		  }
		  private void consume(int minusOne)
		  {
			if (!consumed && minusOne == -1)
			{
			  try
			  {
				EntityUtils.consume(entity);
			  }
			  catch (Exception)
			  {
				// ignored on purpose
			  }
			  consumed = true;
			}
		  }
	  }

	  /// <summary>
	  /// Returns true iff this instance was <seealso cref="#close() closed"/>, otherwise
	  /// returns false. Note that if you override <seealso cref="#close()"/>, you must call
	  /// {@code super.close()}, in order for this instance to be properly closed.
	  /// </summary>
	  protected internal bool Closed
	  {
		  get
		  {
			return closed;
		  }
	  }

	  /// <summary>
	  /// Same as <seealso cref="#doAction(HttpResponse, boolean, Callable)"/> but always do consume at the end.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: protected <T> T doAction(org.apache.http.HttpResponse response, java.util.concurrent.Callable<T> call) throws java.io.IOException
	  protected internal virtual T doAction<T>(HttpResponse response, Callable<T> call)
	  {
		return doAction(response, true, call);
	  }

	  /// <summary>
	  /// Do a specific action and validate after the action that the status is still OK, 
	  /// and if not, attempt to extract the actual server side exception. Optionally
	  /// release the response at exit, depending on <code>consume</code> parameter.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: protected <T> T doAction(org.apache.http.HttpResponse response, boolean consume, java.util.concurrent.Callable<T> call) throws java.io.IOException
	  protected internal virtual T doAction<T>(HttpResponse response, bool consume, Callable<T> call)
	  {
		IOException error = null;
		try
		{
		  return call.call();
		}
		catch (IOException e)
		{
		  error = e;
		}
		catch (Exception e)
		{
		  error = new IOException(e);
		}
		finally
		{
		  try
		  {
			verifyStatus(response);
		  }
		  finally
		  {
			if (consume)
			{
			  try
			  {
				EntityUtils.consume(response.Entity);
			  }
			  catch (Exception)
			  {
				// ignoring on purpose
			  }
			}
		  }
		}
		throw error; // should not get here
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public void close() throws java.io.IOException
	  public virtual void Dispose()
	  {
		closed = true;
	  }

	}

}