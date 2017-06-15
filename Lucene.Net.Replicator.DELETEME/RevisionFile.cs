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
	/// Describes a file in a <seealso cref="Revision"/>. A file has a source, which allows a
	/// single revision to contain files from multiple sources (e.g. multiple
	/// indexes).
	/// 
	/// @lucene.experimental
	/// </summary>
	public class RevisionFile
	{

	  /// <summary>
	  /// The name of the file. </summary>
	  public readonly string fileName;

	  /// <summary>
	  /// The size of the file denoted by <seealso cref="#fileName"/>. </summary>
	  public long size = -1;

	  /// <summary>
	  /// Constructor with the given file name. </summary>
	  public RevisionFile(string fileName)
	  {
		if (string.ReferenceEquals(fileName, null) || fileName.Length == 0)
		{
		  throw new System.ArgumentException("fileName cannot be null or empty");
		}
		this.fileName = fileName;
	  }

	  public override bool Equals(object obj)
	  {
		RevisionFile other = (RevisionFile) obj;
		return fileName.Equals(other.fileName) && size == other.size;
	  }

	  public override int GetHashCode()
	  {
		return fileName.GetHashCode() ^ (int)(size ^ ((long)((ulong)size >> 32)));
	  }

	  public override string ToString()
	  {
		return "fileName=" + fileName + " size=" + size;
	  }

	}

}