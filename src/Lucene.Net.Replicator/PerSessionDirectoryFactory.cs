//STATUS: DRAFT - 4.8.0

using System;
using System.IO;
using Lucene.Net.Store;
using Directory = Lucene.Net.Store.Directory;

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
    /// A <see cref="ISourceDirectoryFactory"/> which returns <see cref="FSDirectory"/> under a
    /// dedicated session directory. When a session is over, the entire directory is
    /// deleted.
    /// </summary>
    /// <remarks>
    /// Lucene.Experimental
    /// </remarks>
    public class PerSessionDirectoryFactory : ISourceDirectoryFactory
    {
        #region Java
        //JAVA: private final File workDir;
        #endregion
        private readonly string workingDirectory;

        /** Constructor with the given sources mapping. */
        public PerSessionDirectoryFactory(string workingDirectory)
        {
            this.workingDirectory = workingDirectory;
        }

        public Directory GetDirectory(string sessionId, string source)
        {
            #region Java
            //JAVA: public Directory getDirectory(String sessionID, String source) throws IOException {
            //JAVA:   File sessionDir = new File(workDir, sessionID);
            //JAVA:   if (!sessionDir.exists() && !sessionDir.mkdirs()) {
            //JAVA:     throw new IOException("failed to create session directory " + sessionDir);
            //JAVA:   }
            //JAVA:   File sourceDir = new File(sessionDir, source);
            //JAVA:   if (!sourceDir.mkdirs()) {
            //JAVA:     throw new IOException("failed to create source directory " + sourceDir);
            //JAVA:   }
            //JAVA:   return FSDirectory.open(sourceDir);
            //JAVA: }
            #endregion

            string sourceDirectory = Path.Combine(workingDirectory, sessionId, source);
            System.IO.Directory.CreateDirectory(sourceDirectory);
            return FSDirectory.Open(sourceDirectory);
        }

        public void CleanupSession(string sessionId)
        {
            if (string.IsNullOrEmpty(sessionId)) throw new ArgumentException("sessionID cannot be empty", "sessionId");

            #region Java
            //JAVA: rm(new File(workDir, sessionID));
            #endregion

            string sessionDirectory = Path.Combine(workingDirectory, sessionId);
            System.IO.Directory.Delete(sessionDirectory, true);
        }

        #region Java
        //JAVA: private void rm(File file) throws IOException {
        //JAVA:   if (file.isDirectory()) {
        //JAVA:     for (File f : file.listFiles()) {
        //JAVA:       rm(f);
        //JAVA:     }
        //JAVA:   }
        //JAVA:   
        //JAVA:   // This should be either an empty directory, or a file
        //JAVA:   if (!file.delete() && file.exists()) {
        //JAVA:     throw new IOException("failed to delete " + file);
        //JAVA:   }
        //JAVA: }
        #endregion
    }
}