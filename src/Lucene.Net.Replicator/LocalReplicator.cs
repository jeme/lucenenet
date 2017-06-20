//STATUS: DRAFT - 4.8.0

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Lucene.Net.Search;
using Lucene.Net.Support;

namespace Lucene.Net.Replicator
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
    /// A <see cref="IReplicator"/> implementation for use by the side that publishes
    /// <see cref="IRevision"/>s, as well for clients to <see cref="CheckForUpdate"/>
    /// check for updates}. When a client needs to be updated, it is returned a
    /// <see cref="SessionToken"/> through which it can
    /// <see cref="ObtainFile"/> the files of that
    /// revision. As long as a revision is being replicated, this replicator
    /// guarantees that it will not be <seealso cref="IRevision.Release"/>.
    /// <para>
    /// Replication sessions expire by default after
    /// <seealso cref="DEFAULT_SESSION_EXPIRATION_THRESHOLD"/>, and the threshold can be
    /// configured through <seealso cref="ExpirationThreshold"/>.
    /// </para>
    /// </summary>
    /// <remarks>
    /// Lucene.Experimental
    /// </remarks>
    public class LocalReplicator : IReplicator
    {
        /// <summary>Threshold for expiring inactive sessions. Defaults to 30 minutes.</summary>
        public const long DEFAULT_SESSION_EXPIRATION_THRESHOLD = 1000 * 60 * 30;

        private long expirationThreshold = DEFAULT_SESSION_EXPIRATION_THRESHOLD;
        private readonly object padlock = new object();
        private volatile RefCountedRevision currentRevision;
        private volatile bool disposed = false;

        private readonly AtomicInt32 sessionToken = new AtomicInt32(0);
        private readonly Dictionary<string, ReplicationSession> sessions = new Dictionary<string, ReplicationSession>();

        /// <summary>
        /// Returns the expiration threshold.
        /// </summary>
        public long ExpirationThreshold
        {
            get { return expirationThreshold; }
            set
            {
                lock (padlock)
                {
                    EnsureOpen();
                    expirationThreshold = value;
                    CheckExpiredSessions();
                }
            }
        }

        public void Publish(IRevision revision)
        {
            #region Java
            //JAVA: public synchronized void publish(Revision revision) throws IOException {
            //JAVA:   ensureOpen();
            //JAVA:   if (currentRevision != null) {
            //JAVA:     int compare = revision.compareTo(currentRevision.revision);
            //JAVA:     if (compare == 0) {
            //JAVA:       // same revision published again, ignore but release it
            //JAVA:       revision.release();
            //JAVA:       return;
            //JAVA:     }
            //JAVA:
            //JAVA:     if (compare < 0) {
            //JAVA:       revision.release();
            //JAVA:       throw new IllegalArgumentException("Cannot publish an older revision: rev=" + revision + " current="
            //JAVA:           + currentRevision);
            //JAVA:     } 
            //JAVA:   }
            //JAVA:
            //JAVA:   // swap revisions
            //JAVA:   final RefCountedRevision oldRevision = currentRevision;
            //JAVA:   currentRevision = new RefCountedRevision(revision);
            //JAVA:   if (oldRevision != null) {
            //JAVA:     oldRevision.decRef();
            //JAVA:   }
            //JAVA:
            //JAVA:   // check for expired sessions
            //JAVA:   checkExpiredSessions();
            //JAVA: } 
            #endregion

            lock (padlock)
            {
                EnsureOpen();

                if (currentRevision != null)
                {
                    int compare = revision.CompareTo(currentRevision.Revision);
                    if (compare == 0)
                    {
                        // same revision published again, ignore but release it
                        revision.Release();
                        return;
                    }

                    if (compare < 0)
                    {
                        revision.Release();
                        throw new ArgumentException(string.Format("Cannot publish an older revision: rev={0} current={1}", revision, currentRevision), "revision");
                    }
                }

                RefCountedRevision oldRevision = currentRevision;
                currentRevision = new RefCountedRevision(revision);
                if(oldRevision != null) 
                    oldRevision.DecRef();

                CheckExpiredSessions();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="currentVersion"></param>
        /// <returns></returns>
        public SessionToken CheckForUpdate(string currentVersion)
        {
            #region Java
            //JAVA: public synchronized SessionToken checkForUpdate(String currentVersion) {
            //JAVA:   ensureOpen();
            //JAVA:   if (currentRevision == null) { // no published revisions yet
            //JAVA:     return null;
            //JAVA:   }
            //JAVA:
            //JAVA:   if (currentVersion != null && currentRevision.revision.compareTo(currentVersion) <= 0) {
            //JAVA:     // currentVersion is newer or equal to latest published revision
            //JAVA:     return null;
            //JAVA:   }
            //JAVA:
            //JAVA:   // currentVersion is either null or older than latest published revision
            //JAVA:   currentRevision.incRef();
            //JAVA:   final String sessionID = Integer.toString(sessionToken.incrementAndGet());
            //JAVA:   final SessionToken sessionToken = new SessionToken(sessionID, currentRevision.revision);
            //JAVA:   final ReplicationSession timedSessionToken = new ReplicationSession(sessionToken, currentRevision);
            //JAVA:   sessions.put(sessionID, timedSessionToken);
            //JAVA:   return sessionToken;
            //JAVA: } 
            #endregion

            lock (padlock)
            {
                EnsureOpen();
                if (currentRevision == null)
                    return null; // no published revisions yet

                if (currentVersion != null && currentRevision.Revision.CompareTo(currentVersion) <= 0)
                    return null; // currentVersion is newer or equal to latest published revision

                // currentVersion is either null or older than latest published revision
                currentRevision.IncRef();

                string sessionID = sessionToken.IncrementAndGet().ToString();
                SessionToken token = new SessionToken(sessionID, currentRevision.Revision);
                sessions[sessionID] = new ReplicationSession(token, currentRevision);
                return token;
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sessionId"></param>
        /// <exception cref="InvalidOperationException"></exception>
        public void Release(string sessionId)
        {
            lock (padlock)
            {
                EnsureOpen();
                ReleaseSession(sessionId);
            }
        }

        public Stream ObtainFile(string sessionId, string source, string fileName)
        {
            #region Java
            //JAVA: public synchronized InputStream obtainFile(String sessionID, String source, String fileName) throws IOException {
            //JAVA:   ensureOpen();
            //JAVA:   ReplicationSession session = sessions.get(sessionID);
            //JAVA:   if (session != null && session.isExpired(expirationThresholdMilllis)) {
            //JAVA:     releaseSession(sessionID);
            //JAVA:     session = null;
            //JAVA:   }
            //JAVA:   // session either previously expired, or we just expired it
            //JAVA:   if (session == null) {
            //JAVA:     throw new SessionExpiredException("session (" + sessionID + ") expired while obtaining file: source=" + source
            //JAVA:         + " file=" + fileName);
            //JAVA:   }
            //JAVA:   sessions.get(sessionID).markAccessed();
            //JAVA:   return session.revision.revision.open(source, fileName);
            //JAVA: }
            #endregion

            lock (padlock)
            {
                EnsureOpen();

                ReplicationSession session = sessions[sessionId];
                if (session != null && session.IsExpired(ExpirationThreshold))
                {
                    ReleaseSession(sessionId);
                    session = null;
                }
                // session either previously expired, or we just expired it
                if (session == null)
                {
                    throw new SessionExpiredException(string.Format("session ({0}) expired while obtaining file: source={1} file={2}", sessionId, source, fileName));
                }
                sessions[sessionId].MarkAccessed();
                return session.Revision.Revision.Open(source, fileName);
            }

        }

        public void Dispose()
        {
            #region Java
            //JAVA: public synchronized void close() throws IOException {
            //JAVA:   if (!closed) {
            //JAVA:     // release all managed revisions
            //JAVA:     for (ReplicationSession session : sessions.values()) {
            //JAVA:       session.revision.decRef();
            //JAVA:     }
            //JAVA:     sessions.clear();
            //JAVA:     closed = true;
            //JAVA:   }
            //JAVA: }
            #endregion

            if (disposed)
                return;

            lock (padlock)
            {
                foreach (ReplicationSession session in sessions.Values)
                    session.Revision.DecRef();
                sessions.Clear();
            }
            disposed = true;
        }

        /// <exception cref="InvalidOperationException"></exception>
        private void CheckExpiredSessions()
        {
            #region Java
            //JAVA: private void checkExpiredSessions() throws IOException {
            //JAVA:   // make a "to-delete" list so we don't risk deleting from the map while iterating it
            //JAVA:   final ArrayList<ReplicationSession> toExpire = new ArrayList<>();
            //JAVA:   for (ReplicationSession token : sessions.values()) {
            //JAVA:     if (token.isExpired(expirationThresholdMilllis)) {
            //JAVA:       toExpire.add(token);
            //JAVA:     }
            //JAVA:   }
            //JAVA:   for (ReplicationSession token : toExpire) {
            //JAVA:     releaseSession(token.session.id);
            //JAVA:   }
            //JAVA: }  
            #endregion
            
            // .NET NOTE: .ToArray() so we don't modify a collection we are enumerating...
            //            I am wondering if it would be overall more practical to switch to a concurrent dictionary...
            foreach (ReplicationSession token in sessions.Values.Where(token => token.IsExpired(ExpirationThreshold)).ToArray())
            {
                ReleaseSession(token.Session.Id);
            }
        }

        /// <exception cref="InvalidOperationException"></exception>
        private void ReleaseSession(string sessionId)
        {
            #region Java
            //JAVA: private void releaseSession(String sessionID) throws IOException {
            //JAVA:   ReplicationSession session = sessions.remove(sessionID);
            //JAVA:   // if we're called concurrently by close() and release(), could be that one
            //JAVA:   // thread beats the other to release the session.
            //JAVA:   if (session != null) {
            //JAVA:     session.revision.decRef();
            //JAVA:   }
            //JAVA: }          
            #endregion

            ReplicationSession session;
            // if we're called concurrently by close() and release(), could be that one
            // thread beats the other to release the session.
            if (sessions.TryGetValue(sessionId, out session))
            {
                sessions.Remove(sessionId);
                session.Revision.DecRef();
            }
        }

        /// <summary>
        /// Ensure that replicator is still open, or throw <see cref="ObjectDisposedException"/> otherwise.
        /// </summary>
        /// <exception cref="ObjectDisposedException">This replicator has already been closed</exception>
        protected void EnsureOpen()
        {
            lock (padlock)
            {
                if (disposed)
                {
                    throw new ObjectDisposedException("This replicator has already been closed");
                }
            }
        }

        private class RefCountedRevision
        {
            private readonly AtomicInt32 refCount = new AtomicInt32(1);

            public IRevision Revision { get; private set; }

            public RefCountedRevision(IRevision revision)
            {
                Revision = revision;
            }

            /// <summary/>
            /// <exception cref="InvalidOperationException"></exception>
            public void DecRef()
            {
                if (refCount.Get() <= 0)
                {
                    //JAVA: throw new IllegalStateException("this revision is already released");
                    throw new InvalidOperationException("this revision is already released");
                }

                var rc = refCount.DecrementAndGet();
                if (rc == 0)
                {
                    bool success = false;
                    try
                    {
                        Revision.Release();
                        success = true;
                    }
                    finally
                    {
                        if (!success)
                        {
                            // Put reference back on failure
                            refCount.IncrementAndGet();
                        }
                    }
                }
                else if (rc < 0)
                {
                    //JAVA: throw new IllegalStateException("too many decRef calls: refCount is " + rc + " after decrement");
                    throw new InvalidOperationException(string.Format("too many decRef calls: refCount is {0} after decrement", rc));
                }
            }

            public void IncRef()
            {
                refCount.IncrementAndGet();
            }       
        }

        private class ReplicationSession
        {
            public SessionToken Session { get; private set; }
            public RefCountedRevision Revision { get; private set; }

            private long lastAccessTime;

            public ReplicationSession(SessionToken session, RefCountedRevision revision)
            {
                Session = session;
                Revision = revision;
                //JAVA: lastAccessTime = System.currentTimeMillis();
                lastAccessTime = Stopwatch.GetTimestamp();
            }

            public bool IsExpired(long expirationThreshold)
            {
                //JAVA: return lastAccessTime < (System.currentTimeMillis() - expirationThreshold);
                return lastAccessTime < Stopwatch.GetTimestamp() - expirationThreshold * Stopwatch.Frequency / 1000;
            }

            public void MarkAccessed()
            {
                //JAVA: lastAccessTime = System.currentTimeMillis();
                lastAccessTime = Stopwatch.GetTimestamp();
            }
        }

    }
}