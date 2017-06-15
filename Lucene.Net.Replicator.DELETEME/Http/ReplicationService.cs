using System;
using System.Collections.Generic;

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



	using HttpStatus = org.apache.http.HttpStatus;

	/// <summary>
	/// A server-side service for handling replication requests. The service assumes
	/// requests are sent in the format
	/// <code>/&lt;context&gt;/&lt;shard&gt;/&lt;action&gt;</code> where
	/// <ul>
	/// <li>{@code context} is the servlet context, e.g. <seealso cref="#REPLICATION_CONTEXT"/>
	/// <li>{@code shard} is the ID of the shard, e.g. "s1"
	/// <li>{@code action} is one of <seealso cref="ReplicationAction"/> values
	/// </ul>
	/// For example, to check whether there are revision updates for shard "s1" you
	/// should send the request: <code>http://host:port/replicate/s1/update</code>.
	/// <para>
	/// This service is written like a servlet, and
	/// <seealso cref="#perform(HttpServletRequest, HttpServletResponse)"/> takes servlet
	/// request and response accordingly, so it is quite easy to embed in your
	/// application's servlet.
	/// 
	/// @lucene.experimental
	/// </para>
	/// </summary>
	public class ReplicationService
	{

	  /// <summary>
	  /// Actions supported by the <seealso cref="ReplicationService"/>. </summary>
	  public enum ReplicationAction
	  {
		OBTAIN,
		RELEASE,
		UPDATE
	  }

	  /// <summary>
	  /// The context path for the servlet. </summary>
	  public const string REPLICATION_CONTEXT = "/replicate";

	  /// <summary>
	  /// Request parameter name for providing the revision version. </summary>
	  public const string REPLICATE_VERSION_PARAM = "version";

	  /// <summary>
	  /// Request parameter name for providing a session ID. </summary>
	  public const string REPLICATE_SESSION_ID_PARAM = "sessionid";

	  /// <summary>
	  /// Request parameter name for providing the file's source. </summary>
	  public const string REPLICATE_SOURCE_PARAM = "source";

	  /// <summary>
	  /// Request parameter name for providing the file's name. </summary>
	  public const string REPLICATE_FILENAME_PARAM = "filename";

	  private const int SHARD_IDX = 0, ACTION_IDX = 1;

	  private readonly IDictionary<string, Replicator> replicators;

	  public ReplicationService(IDictionary<string, Replicator> replicators) : base()
	  {
		this.replicators = replicators;
	  }

	  /// <summary>
	  /// Returns the path elements that were given in the servlet request, excluding
	  /// the servlet's action context.
	  /// </summary>
	  private string[] getPathElements(HttpServletRequest req)
	  {
		string path = req.ServletPath;
		string pathInfo = req.PathInfo;
		if (!string.ReferenceEquals(pathInfo, null))
		{
		  path += pathInfo;
		}
		int actionLen = REPLICATION_CONTEXT.Length;
		int startIdx = actionLen;
		if (path.Length > actionLen && path[actionLen] == '/')
		{
		  ++startIdx;
		}

		// split the string on '/' and remove any empty elements. This is better
		// than using String.split() since the latter may return empty elements in
		// the array
		StringTokenizer stok = new StringTokenizer(path.Substring(startIdx), "/");
		List<string> elements = new List<string>();
		while (stok.hasMoreTokens())
		{
		  elements.Add(stok.nextToken());
		}
		return elements.ToArray();
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private static String extractRequestParam(javax.servlet.http.HttpServletRequest req, String paramName) throws javax.servlet.ServletException
	  private static string extractRequestParam(HttpServletRequest req, string paramName)
	  {
		string param = req.getParameter(paramName);
		if (string.ReferenceEquals(param, null))
		{
		  throw new ServletException("Missing mandatory parameter: " + paramName);
		}
		return param;
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private static void copy(java.io.InputStream in, java.io.OutputStream out) throws java.io.IOException
	  private static void copy(System.IO.Stream @in, System.IO.Stream @out)
	  {
		sbyte[] buf = new sbyte[16384];
		int numRead;
		while ((numRead = @in.Read(buf, 0, buf.Length)) != -1)
		{
		  @out.Write(buf, 0, numRead);
		}
	  }

	  /// <summary>
	  /// Executes the replication task. </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void perform(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse resp) throws javax.servlet.ServletException, java.io.IOException
	  public virtual void perform(HttpServletRequest req, HttpServletResponse resp)
	  {
		string[] pathElements = getPathElements(req);

		if (pathElements.Length != 2)
		{
		  throw new ServletException("invalid path, must contain shard ID and action, e.g. */s1/update");
		}

//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final ReplicationAction action;
		ReplicationAction action;
		try
		{
		  action = Enum.Parse(typeof(ReplicationAction), pathElements[ACTION_IDX].ToUpper(Locale.ENGLISH));
		}
		catch (System.ArgumentException)
		{
		  throw new ServletException("Unsupported action provided: " + pathElements[ACTION_IDX]);
		}

//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.apache.lucene.replicator.Replicator replicator = replicators.get(pathElements[SHARD_IDX]);
		Replicator replicator = replicators[pathElements[SHARD_IDX]];
		if (replicator == null)
		{
		  throw new ServletException("unrecognized shard ID " + pathElements[SHARD_IDX]);
		}

		ServletOutputStream resOut = resp.OutputStream;
		try
		{
		  switch (action)
		  {
			case org.apache.lucene.replicator.http.ReplicationService.ReplicationAction.OBTAIN:
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final String sessionID = extractRequestParam(req, REPLICATE_SESSION_ID_PARAM);
			  string sessionID = extractRequestParam(req, REPLICATE_SESSION_ID_PARAM);
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final String fileName = extractRequestParam(req, REPLICATE_FILENAME_PARAM);
			  string fileName = extractRequestParam(req, REPLICATE_FILENAME_PARAM);
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final String source = extractRequestParam(req, REPLICATE_SOURCE_PARAM);
			  string source = extractRequestParam(req, REPLICATE_SOURCE_PARAM);
			  System.IO.Stream @in = replicator.obtainFile(sessionID, source, fileName);
			  try
			  {
				copy(@in, resOut);
			  }
			  finally
			  {
				@in.Close();
			  }
			  break;
			case org.apache.lucene.replicator.http.ReplicationService.ReplicationAction.RELEASE:
			  replicator.release(extractRequestParam(req, REPLICATE_SESSION_ID_PARAM));
			  break;
			case org.apache.lucene.replicator.http.ReplicationService.ReplicationAction.UPDATE:
			  string currVersion = req.getParameter(REPLICATE_VERSION_PARAM);
			  SessionToken token = replicator.checkForUpdate(currVersion);
			  if (token == null)
			  {
				resOut.write(0); // marker for null token
			  }
			  else
			  {
				resOut.write(1); // marker for null token
				token.serialize(new DataOutputStream(resOut));
			  }
			  break;
		  }
		}
		catch (Exception e)
		{
		  resp.Status = HttpStatus.SC_INTERNAL_SERVER_ERROR; // propagate the failure
		  try
		  {
			/*
			 * Note: it is assumed that "identified exceptions" are thrown before
			 * anything was written to the stream.
			 */
			ObjectOutputStream oos = new ObjectOutputStream(resOut);
			oos.writeObject(e);
			oos.flush();
		  }
		  catch (Exception e2)
		  {
			throw new IOException("Could not serialize", e2);
		  }
		}
		finally
		{
		  resp.flushBuffer();
		}
	  }

	}

}