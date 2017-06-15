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


	/// <summary>
	/// An interface for replicating files. Allows a producer to
	/// <seealso cref="#publish(Revision) publish"/> <seealso cref="Revision"/>s and consumers to
	/// <seealso cref="#checkForUpdate(String) check for updates"/>. When a client needs to be
	/// updated, it is given a <seealso cref="SessionToken"/> through which it can
	/// <seealso cref="#obtainFile(String, String, String) obtain"/> the files of that
	/// revision. After the client has finished obtaining all the files, it should
	/// <seealso cref="#release(String) release"/> the given session, so that the files can be
	/// reclaimed if they are not needed anymore.
	/// <para>
	/// A client is always updated to the newest revision available. That is, if a
	/// client is on revision <em>r1</em> and revisions <em>r2</em> and <em>r3</em>
	/// were published, then when the cllient will next check for update, it will
	/// receive <em>r3</em>.
	/// 
	/// @lucene.experimental
	/// </para>
	/// </summary>
	public interface Replicator : System.IDisposable
	{

	  /// <summary>
	  /// Publish a new <seealso cref="Revision"/> for consumption by clients. It is the
	  /// caller's responsibility to verify that the revision files exist and can be
	  /// read by clients. When the revision is no longer needed, it will be
	  /// <seealso cref="Revision#release() released"/> by the replicator.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void publish(Revision revision) throws java.io.IOException;
	  void publish(Revision revision);

	  /// <summary>
	  /// Check whether the given version is up-to-date and returns a
	  /// <seealso cref="SessionToken"/> which can be used for fetching the revision files,
	  /// otherwise returns {@code null}.
	  /// <para>
	  /// <b>NOTE:</b> when the returned session token is no longer needed, you
	  /// should call <seealso cref="#release(String)"/> so that the session resources can be
	  /// reclaimed, including the revision files.
	  /// </para>
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public SessionToken checkForUpdate(String currVersion) throws java.io.IOException;
	  SessionToken checkForUpdate(string currVersion);

	  /// <summary>
	  /// Notify that the specified <seealso cref="SessionToken"/> is no longer needed by the
	  /// caller.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void release(String sessionID) throws java.io.IOException;
	  void release(string sessionID);

	  /// <summary>
	  /// Returns an <seealso cref="InputStream"/> for the requested file and source in the
	  /// context of the given <seealso cref="SessionToken#id session"/>.
	  /// <para>
	  /// <b>NOTE:</b> it is the caller's responsibility to close the returned
	  /// stream.
	  /// 
	  /// </para>
	  /// </summary>
	  /// <exception cref="SessionExpiredException"> if the specified session has already
	  ///         expired </exception>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public java.io.InputStream obtainFile(String sessionID, String source, String fileName) throws java.io.IOException;
	  System.IO.Stream obtainFile(string sessionID, string source, string fileName);

	}

}