//STATUS: DRAFT - 4.8.0

using System;
using System.IO;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Lucene.Net.Index;
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
    /// A <see cref="IRevision"/> of a single index files which comprises the list of files
    /// that are part of the current <see cref="IndexCommit"/>. To ensure the files are not
    /// deleted by <see cref="IndexWriter"/> for as long as this revision stays alive (i.e.
    /// until <see cref="Release"/>, the current commit point is snapshotted, using
    /// <see cref="SnapshotDeletionPolicy"/> (this means that the given writer's
    /// <see cref="IndexWriterConfig.IndexDeletionPolicy"/> should return
    /// <see cref="SnapshotDeletionPolicy"/>).
    /// <p>
    /// When this revision is <see cref="Release"/>d, it releases the obtained
    /// snapshot as well as calls <see cref="IndexWriter.DeleteUnusedFiles"/> so that the
    /// snapshotted files are deleted (if they are no longer needed).
    /// </p>
    /// </summary>
    /// <remarks>
    /// Lucene.Experimental
    /// </remarks>
    public class IndexRevision : IRevision
    {
        #region Java
        //JAVA: private static final int RADIX = 16;
        //JAVA: private static final String SOURCE = "index";
        //JAVA: private final IndexWriter writer;
        //JAVA: private final IndexCommit commit;
        //JAVA: private final SnapshotDeletionPolicy sdp;
        //JAVA: private final String version;
        //JAVA: private final Map<String, List<RevisionFile>> sourceFiles;
        #endregion

        private const string SOURCE = "index";

        private readonly IndexWriter writer;
        private readonly IndexCommit commit;
        private readonly SnapshotDeletionPolicy sdp;

        public string Version { get; private set; }
        public IReadOnlyDictionary<string, IReadOnlyCollection<RevisionFile>> SourceFiles { get; private set; }

        public IndexRevision(IndexWriter writer)
        {
            #region Java
            //JAVA: public IndexRevision(IndexWriter writer) throws IOException {
            //JAVA:   IndexDeletionPolicy delPolicy = writer.getConfig().getIndexDeletionPolicy();
            //JAVA:   if (!(delPolicy instanceof SnapshotDeletionPolicy)) {
            //JAVA:     throw new IllegalArgumentException("IndexWriter must be created with SnapshotDeletionPolicy");
            //JAVA:   }
            //JAVA:   this.writer = writer;
            //JAVA:   this.sdp = (SnapshotDeletionPolicy) delPolicy;
            //JAVA:   this.commit = sdp.snapshot();
            //JAVA:   this.version = revisionVersion(commit);
            //JAVA:   this.sourceFiles = revisionFiles(commit);
            //JAVA: }              
            #endregion

            sdp = writer.Config.IndexDeletionPolicy as SnapshotDeletionPolicy;
            if (sdp == null)
                throw new ArgumentException("IndexWriter must be created with SnapshotDeletionPolicy", "writer");

            this.writer = writer;
            this.commit = sdp.Snapshot();
            this.Version = RevisionVersion(commit);
            this.SourceFiles = RevisionFiles(commit);
        }

        public int CompareTo(string version)
        {
            #region Java
            //JAVA: long gen = Long.parseLong(version, RADIX);
            //JAVA: long commitGen = commit.getGeneration();
            //JAVA: return commitGen < gen ? -1 : (commitGen > gen ? 1 : 0);
            #endregion
            long gen = long.Parse(version, NumberStyles.HexNumber);
            long commitGen = commit.Generation;
            //TODO: long.CompareTo(); but which goes where.
            return commitGen < gen ? -1 : (commitGen > gen ? 1 : 0);
        }

        public int CompareTo(IRevision other)
        {
            #region Java
            //JAVA: IndexRevision other = (IndexRevision)o;
            //JAVA: return commit.compareTo(other.commit);
            #endregion
            //TODO: This breaks the contract and will fail if called with a different implementation
            //      This is a flaw inherited from the original source...
            //      It should at least provide a better description to the InvalidCastException
            IndexRevision or = (IndexRevision)other;
            return commit.CompareTo(or.commit);
        }

        public Stream Open(string source, string fileName)
        {
            Debug.Assert(source.Equals(SOURCE), string.Format("invalid source; expected={0} got={1}", SOURCE, source));
            return new IndexInputInputStream(commit.Directory.OpenInput(fileName, IOContext.READ_ONCE));
        }

        public void Release()
        {
            sdp.Release(commit);
            writer.DeleteUnusedFiles();
        }

        public override string ToString()
        {
            return "IndexRevision version=" + Version + " files=" + SourceFiles;
        }

        // returns a RevisionFile with some metadata
        private static RevisionFile CreateRevisionFile(string fileName, Directory directory)
        {
            #region Java
            //JAVA: private static RevisionFile newRevisionFile(String file, Directory dir) throws IOException {
            //JAVA:   RevisionFile revFile = new RevisionFile(file);
            //JAVA:   revFile.size = dir.fileLength(file);
            //JAVA:   return revFile;
            //JAVA: }             
            #endregion
            return new RevisionFile(fileName, directory.FileLength(fileName));
        }

        /** Returns a singleton map of the revision files from the given {@link IndexCommit}. */
        public static IReadOnlyDictionary<string, IReadOnlyCollection<RevisionFile>> RevisionFiles(IndexCommit commit)
        {
            #region Java
            //JAVA: public static Map<String,List<RevisionFile>> revisionFiles(IndexCommit commit) throws IOException {
            //JAVA:   Collection<String> commitFiles = commit.getFileNames();
            //JAVA:   List<RevisionFile> revisionFiles = new ArrayList<>(commitFiles.size());
            //JAVA:   String segmentsFile = commit.getSegmentsFileName();
            //JAVA:   Directory dir = commit.getDirectory();
            //JAVA:   
            //JAVA:   for (String file : commitFiles) {
            //JAVA:     if (!file.equals(segmentsFile)) {
            //JAVA:       revisionFiles.add(newRevisionFile(file, dir));
            //JAVA:     }
            //JAVA:   }
            //JAVA:   revisionFiles.add(newRevisionFile(segmentsFile, dir)); // segments_N must be last
            //JAVA:   return Collections.singletonMap(SOURCE, revisionFiles);
            //JAVA: }
            #endregion

            IEnumerable<RevisionFile> revisionFiles = commit.FileNames
                .Where(file => !string.Equals(file, commit.SegmentsFileName))
                .Select(file => CreateRevisionFile(file, commit.Directory))
                //Note: segments_N must be last
                .Union(new[] { CreateRevisionFile(commit.SegmentsFileName, commit.Directory) });
            return new ReadOnlyDictionary<string, IReadOnlyCollection<RevisionFile>>(new Dictionary<string, IReadOnlyCollection<RevisionFile>>
            {
                { SOURCE, revisionFiles.ToList().AsReadOnly() }
            });
        }
   
        /// <summary>
        /// Returns a String representation of a revision's version from the given <see cref="IndexCommit"/>
        /// </summary>
        /// <param name="commit"></param>
        /// <returns></returns>
        public static string RevisionVersion(IndexCommit commit)
        {
            #region Java
            //JAVA: public static String revisionVersion(IndexCommit commit) {
            //JAVA:   return Long.toString(commit.getGeneration(), RADIX);
            //JAVA: }  
            #endregion           
            return commit.Generation.ToString("X");
        }
    }
}