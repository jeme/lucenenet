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


	/// <summary>
	/// Token for a replication session, for guaranteeing that source replicated
	/// files will be kept safe until the replication completes.
	/// </summary>
	/// <seealso cref= Replicator#checkForUpdate(String) </seealso>
	/// <seealso cref= Replicator#release(String) </seealso>
	/// <seealso cref= LocalReplicator#DEFAULT_SESSION_EXPIRATION_THRESHOLD
	/// 
	/// @lucene.experimental </seealso>
	public sealed class SessionToken
	{

	  /// <summary>
	  /// ID of this session.
	  /// Should be passed when releasing the session, thereby acknowledging the 
	  /// <seealso cref="Replicator Replicator"/> that this session is no longer in use. </summary>
	  /// <seealso cref= Replicator#release(String) </seealso>
	  public readonly string id;

	  /// <seealso cref= Revision#getVersion() </seealso>
	  public readonly string version;

	  /// <seealso cref= Revision#getSourceFiles() </seealso>
	  public readonly IDictionary<string, IList<RevisionFile>> sourceFiles;

	  /// <summary>
	  /// Constructor which deserializes from the given <seealso cref="DataInput"/>. </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public SessionToken(java.io.DataInput in) throws java.io.IOException
	  public SessionToken(DataInput @in)
	  {
		this.id = @in.readUTF();
		this.version = @in.readUTF();
		this.sourceFiles = new Dictionary<>();
		int numSources = @in.readInt();
		while (numSources > 0)
		{
		  string source = @in.readUTF();
		  int numFiles = @in.readInt();
		  IList<RevisionFile> files = new List<RevisionFile>(numFiles);
		  for (int i = 0; i < numFiles; i++)
		  {
			string fileName = @in.readUTF();
			RevisionFile file = new RevisionFile(fileName);
			file.size = @in.readLong();
			files.Add(file);
		  }
		  this.sourceFiles[source] = files;
		  --numSources;
		}
	  }

	  /// <summary>
	  /// Constructor with the given id and revision. </summary>
	  public SessionToken(string id, Revision revision)
	  {
		this.id = id;
		this.version = revision.Version;
		this.sourceFiles = revision.SourceFiles;
	  }

	  /// <summary>
	  /// Serialize the token data for communication between server and client. </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void serialize(java.io.DataOutput out) throws java.io.IOException
	  public void serialize(DataOutput @out)
	  {
		@out.writeUTF(id);
		@out.writeUTF(version);
		@out.writeInt(sourceFiles.Count);
		foreach (KeyValuePair<string, IList<RevisionFile>> e in sourceFiles.SetOfKeyValuePairs())
		{
		  @out.writeUTF(e.Key);
		  IList<RevisionFile> files = e.Value;
		  @out.writeInt(files.Count);
		  foreach (RevisionFile file in files)
		  {
			@out.writeUTF(file.fileName);
			@out.writeLong(file.size);
		  }
		}
	  }

	  public override string ToString()
	  {
		return "id=" + id + " version=" + version + " files=" + sourceFiles;
	  }

	}
}