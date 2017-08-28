﻿namespace Lucene.Net.Replicator.Http.Abstractions
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
    /// Abstraction for remote replication requests, allows easy integration into any hosting frameworks.
    /// </summary>
    /// <remarks>
    /// .NET Specific Abstraction  
    /// </remarks>
    //Note: LUCENENET specific
    public interface IReplicationRequest
    {
        /// <summary>
        /// Provides the requested path which mapps to a replication operation.
        /// </summary>
        string Path { get; }

        /// <summary>
        /// Returns the requested parameter or null if not present.
        /// </summary>
        /// <remarks>
        /// May through execeptions if the same parameter is provided multiple times, consult the documentation for the specific implementation.
        /// </remarks>
        /// <param name="name">the name of the requested parameter</param>
        /// <returns>the value of the requested parameter or null if not present</returns>
        string Parameter(string name);
    }
}