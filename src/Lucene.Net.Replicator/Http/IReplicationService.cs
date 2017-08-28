using System;
using Lucene.Net.Replicator.Http.Abstractions;

namespace Lucene.Net.Replicator.Http
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
    /// A server-side service for handling replication requests. The service assumes
    /// requests are sent in the format <code>/&lt;context&gt;/&lt;shard&gt;/&lt;action&gt;</code> where
    /// <ul>
    ///   <li><code>context</code> is the servlet context, e.g. <see cref="REPLICATION_CONTEXT"/></li>
    ///   <li><code>shard</code> is the ID of the shard, e.g. "s1"</li>
    ///   <li><code>action</code> is one of <see cref="ReplicationService.ReplicationAction"/> values</li>
    /// </ul>
    /// For example, to check whether there are revision updates for shard "s1" you
    /// should send the request: <code>http://host:port/replicate/s1/update</code>.
    /// </summary>
    /// <remarks>
    /// This service is written using abstractions over requests and responses which makes it easy
    /// to integrate into any hosting framework.
    /// <p>
    /// See the Lucene.Net.Replicator.AspNetCore for an example of an implementation for the AspNetCore Framework.
    /// </p> 
    /// </remarks>
    /// <remarks>
    /// Lucene.Experimental
    /// </remarks>
    public interface IReplicationService
    {
        /// <summary>
        /// Executes the replication task.
        /// </summary>
        /// <exception cref="InvalidOperationException">required parameters are missing</exception>
        void Perform(IReplicationRequest request, IReplicationResponse response);
    }
}