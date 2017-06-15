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


	using SourceDirectoryFactory = org.apache.lucene.replicator.ReplicationClient.SourceDirectoryFactory;
	using Directory = org.apache.lucene.store.Directory;
	using FSDirectory = org.apache.lucene.store.FSDirectory;

	/// <summary>
	/// A <seealso cref="SourceDirectoryFactory"/> which returns <seealso cref="FSDirectory"/> under a
	/// dedicated session directory. When a session is over, the entire directory is
	/// deleted.
	/// 
	/// @lucene.experimental
	/// </summary>
	public class PerSessionDirectoryFactory : SourceDirectoryFactory
	{

	  private readonly File workDir;

	  /// <summary>
	  /// Constructor with the given sources mapping. </summary>
	  public PerSessionDirectoryFactory(File workDir)
	  {
		this.workDir = workDir;
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private void rm(java.io.File file) throws java.io.IOException
	  private void rm(File file)
	  {
		if (file.Directory)
		{
		  foreach (File f in file.listFiles())
		  {
			rm(f);
		  }
		}

		// This should be either an empty directory, or a file
		if (!file.delete() && file.exists())
		{
		  throw new IOException("failed to delete " + file);
		}
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public org.apache.lucene.store.Directory getDirectory(String sessionID, String source) throws java.io.IOException
	  public virtual Directory getDirectory(string sessionID, string source)
	  {
		File sessionDir = new File(workDir, sessionID);
		if (!sessionDir.exists() && !sessionDir.mkdirs())
		{
		  throw new IOException("failed to create session directory " + sessionDir);
		}
		File sourceDir = new File(sessionDir, source);
		if (!sourceDir.mkdirs())
		{
		  throw new IOException("failed to create source directory " + sourceDir);
		}
		return FSDirectory.open(sourceDir);
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public void cleanupSession(String sessionID) throws java.io.IOException
	  public virtual void cleanupSession(string sessionID)
	  {
		if (sessionID.Length == 0)
		{ // protect against deleting workDir entirely!
		  throw new System.ArgumentException("sessionID cannot be empty");
		}
		rm(new File(workDir, sessionID));
	  }

	}

}