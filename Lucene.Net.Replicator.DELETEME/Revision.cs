﻿using System;
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


	using IndexInput = org.apache.lucene.store.IndexInput;

	/// <summary>
	/// A revision comprises lists of files that come from different sources and need
	/// to be replicated together to e.g. guarantee that all resources are in sync.
	/// In most cases an application will replicate a single index, and so the
	/// revision will contain files from a single source. However, some applications
	/// may require to treat a collection of indexes as a single entity so that the
	/// files from all sources are replicated together, to guarantee consistency
	/// beween them. For example, an application which indexes facets will need to
	/// replicate both the search and taxonomy indexes together, to guarantee that
	/// they match at the client side.
	/// 
	/// @lucene.experimental
	/// </summary>
	public interface Revision : IComparable<Revision>
	{

	  /// <summary>
	  /// Compares the revision to the given version string. Behaves like
	  /// <seealso cref="Comparable#compareTo(Object)"/>.
	  /// </summary>
	  int compareTo(string version);

	  /// <summary>
	  /// Returns a string representation of the version of this revision. The
	  /// version is used by <seealso cref="#compareTo(String)"/> as well as to
	  /// serialize/deserialize revision information. Therefore it must be self
	  /// descriptive as well as be able to identify one revision from another.
	  /// </summary>
	  string Version {get;}

	  /// <summary>
	  /// Returns the files that comprise this revision, as a mapping from a source
	  /// to a list of files.
	  /// </summary>
	  IDictionary<string, IList<RevisionFile>> SourceFiles {get;}

	  /// <summary>
	  /// Returns an <seealso cref="IndexInput"/> for the given fileName and source. It is the
	  /// caller's respnsibility to close the <seealso cref="IndexInput"/> when it has been
	  /// consumed.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public java.io.InputStream open(String source, String fileName) throws java.io.IOException;
	  System.IO.Stream open(string source, string fileName);

	  /// <summary>
	  /// Called when this revision can be safely released, i.e. where there are no
	  /// more references to it.
	  /// </summary>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void release() throws java.io.IOException;
	  void release();

	}

}