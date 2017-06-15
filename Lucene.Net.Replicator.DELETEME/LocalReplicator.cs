using System;
using System.Collections.Generic;

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


	using AlreadyClosedException = org.apache.lucene.store.AlreadyClosedException;

	/// <summary>
	/// A <seealso cref="Replicator"/> implementation for use by the side that publishes
	/// <seealso cref="Revision"/>s, as well for clients to {@link #checkForUpdate(String)
	/// check for updates}. When a client needs to be updated, it is returned a
	/// <seealso cref="SessionToken"/> through which it can
	/// <seealso cref="#obtainFile(String, String, String) obtain"/> the files of that
	/// revision. As long as a revision is being replicated, this replicator
	/// guarantees that it will not be <seealso cref="Revision#release() released"/>.
	/// <para>
	/// Replication sessions expire by default after
	/// <seealso cref="#DEFAULT_SESSION_EXPIRATION_THRESHOLD"/>, and the threshold can be
	/// configured through <seealso cref="#setExpirationThreshold(long)"/>.
	/// 
	/// @lucene.experimental
	/// </para>
	/// </summary>
	public class LocalReplicator : Replicator
	{

	  private class RefCountedRevision
	  {
		internal readonly AtomicInteger refCount = new AtomicInteger(1);
		public readonly Revision revision;

		public RefCountedRevision(Revision revision)
		{
		  this.revision = revision;
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void decRef() throws java.io.IOException
		public virtual void decRef()
		{
		  if (refCount.get() <= 0)
		  {
			throw new System.InvalidOperationException("this revision is already released");
		  }

//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final int rc = refCount.decrementAndGet();
		  int rc = refCount.decrementAndGet();
		  if (rc == 0)
		  {
			bool success = false;
			try
			{
			  revision.release();
			  success = true;
			}
			finally
			{
			  if (!success)
			  {
				// Put reference back on failure
				refCount.incrementAndGet();
			  }
			}
		  }
		  else if (rc < 0)
		  {
			throw new System.InvalidOperationException("too many decRef calls: refCount is " + rc + " after decrement");
		  }
		}

		public virtual void incRef()
		{
		  refCount.incrementAndGet();
		}

	  }

	  private class ReplicationSession
	  {
		public readonly SessionToken session;
		public readonly RefCountedRevision revision;
		internal volatile long lastAccessTime;

		internal ReplicationSession(SessionToken session, RefCountedRevision revision)
		{
		  this.session = session;
		  this.revision = revision;
		  lastAccessTime = DateTimeHelperClass.CurrentUnixTimeMillis();
		}

		internal virtual bool isExpired(long expirationThreshold)
		{
		  return lastAccessTime < (DateTimeHelperClass.CurrentUnixTimeMillis() - expirationThreshold);
		}

		internal virtual void markAccessed()
		{
		  lastAccessTime = DateTimeHelperClass.CurrentUnixTimeMillis();
		}
	  }

	  /// <summary>
	  /// Threshold for expiring inactive sessions. Defaults to 30 minutes. </summary>
	  public const long DEFAULT_SESSION_EXPIRATION_THRESHOLD = 1000 * 60 * 30;

	  private long expirationThresholdMilllis = LocalReplicator.DEFAULT_SESSION_EXPIRATION_THRESHOLD;

	  private volatile RefCountedRevision currentRevision;
	  private volatile bool closed = false;

	  private readonly AtomicInteger sessionToken = new AtomicInteger(0);
	  private readonly IDictionary<string, ReplicationSession> sessions = new Dictionary<string, ReplicationSession>();

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private void checkExpiredSessions() throws java.io.IOException
	  private void checkExpiredSessions()
	  {
		// make a "to-delete" list so we don't risk deleting from the map while iterating it
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final java.util.ArrayList<ReplicationSession> toExpire = new java.util.ArrayList<>();
		List<ReplicationSession> toExpire = new List<ReplicationSession>();
		foreach (ReplicationSession token in sessions.Values)
		{
		  if (token.isExpired(expirationThresholdMilllis))
		  {
			toExpire.Add(token);
		  }
		}
		foreach (ReplicationSession token in toExpire)
		{
		  releaseSession(token.session.id);
		}
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private void releaseSession(String sessionID) throws java.io.IOException
	  private void releaseSession(string sessionID)
	  {
		ReplicationSession session = sessions.Remove(sessionID);
		// if we're called concurrently by close() and release(), could be that one
		// thread beats the other to release the session.
		if (session != null)
		{
		  session.revision.decRef();
		}
	  }

	  /// <summary>
	  /// Ensure that replicator is still open, or throw <seealso cref="AlreadyClosedException"/> otherwise. </summary>
	  protected internal void ensureOpen()
	  {
		  lock (this)
		  {
			if (closed)
			{
			  throw new AlreadyClosedException("This replicator has already been closed");
			}
		  }
	  }

	  public override SessionToken checkForUpdate(string currentVersion)
	  {
		  lock (this)
		  {
			ensureOpen();
			if (currentRevision == null)
			{ // no published revisions yet
			  return null;
			}
        
			if (!string.ReferenceEquals(currentVersion, null) && currentRevision.revision.compareTo(currentVersion) <= 0)
			{
			  // currentVersion is newer or equal to latest published revision
			  return null;
			}
        
			// currentVersion is either null or older than latest published revision
			currentRevision.incRef();
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final String sessionID = Convert.ToString(sessionToken.incrementAndGet());
			string sessionID = Convert.ToString(sessionToken.incrementAndGet());
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final SessionToken sessionToken = new SessionToken(sessionID, currentRevision.revision);
			SessionToken sessionToken = new SessionToken(sessionID, currentRevision.revision);
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final ReplicationSession timedSessionToken = new ReplicationSession(sessionToken, currentRevision);
			ReplicationSession timedSessionToken = new ReplicationSession(sessionToken, currentRevision);
			sessions[sessionID] = timedSessionToken;
			return sessionToken;
		  }
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public synchronized void close() throws java.io.IOException
	  public override void close()
	  {
		  lock (this)
		  {
			if (!closed)
			{
			  // release all managed revisions
			  foreach (ReplicationSession session in sessions.Values)
			  {
				session.revision.decRef();
			  }
			  sessions.Clear();
			  closed = true;
			}
		  }
	  }

	  /// <summary>
	  /// Returns the expiration threshold.
	  /// </summary>
	  /// <seealso cref= #setExpirationThreshold(long) </seealso>
	  public virtual long ExpirationThreshold
	  {
		  get
		  {
			return expirationThresholdMilllis;
		  }
		  set
		  {
			  lock (this)
			  {
				ensureOpen();
				this.expirationThresholdMilllis = value;
				checkExpiredSessions();
			  }
		  }
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public synchronized java.io.InputStream obtainFile(String sessionID, String source, String fileName) throws java.io.IOException
	  public override System.IO.Stream obtainFile(string sessionID, string source, string fileName)
	  {
		  lock (this)
		  {
			ensureOpen();
			ReplicationSession session = sessions[sessionID];
			if (session != null && session.isExpired(expirationThresholdMilllis))
			{
			  releaseSession(sessionID);
			  session = null;
			}
			// session either previously expired, or we just expired it
			if (session == null)
			{
			  throw new SessionExpiredException("session (" + sessionID + ") expired while obtaining file: source=" + source + " file=" + fileName);
			}
			sessions[sessionID].markAccessed();
			return session.revision.revision.open(source, fileName);
		  }
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public synchronized void publish(Revision revision) throws java.io.IOException
	  public override void publish(Revision revision)
	  {
		  lock (this)
		  {
			ensureOpen();
			if (currentRevision != null)
			{
			  int compare = revision.compareTo(currentRevision.revision);
			  if (compare == 0)
			  {
				// same revision published again, ignore but release it
				revision.release();
				return;
			  }
        
			  if (compare < 0)
			  {
				revision.release();
				throw new System.ArgumentException("Cannot publish an older revision: rev=" + revision + " current=" + currentRevision);
			  }
			}
        
			// swap revisions
//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final RefCountedRevision oldRevision = currentRevision;
			RefCountedRevision oldRevision = currentRevision;
			currentRevision = new RefCountedRevision(revision);
			if (oldRevision != null)
			{
			  oldRevision.decRef();
			}
        
			// check for expired sessions
			checkExpiredSessions();
		  }
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public synchronized void release(String sessionID) throws java.io.IOException
	  public override void release(string sessionID)
	  {
		  lock (this)
		  {
			ensureOpen();
			releaseSession(sessionID);
		  }
	  }


	}

}