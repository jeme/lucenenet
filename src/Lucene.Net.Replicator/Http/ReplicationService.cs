﻿using System;
using System.Collections.Generic;
using System.IO;
using Lucene.Net.Replicator.Http.Abstractions;
using Lucene.Net.Support.IO;
using Newtonsoft.Json;

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
    ///   <li><code>action</code> is one of <see cref="ReplicationAction"/> values</li>
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
    public class ReplicationService 
    {
        /// <summary>
        /// Actions supported by the <see cref="ReplicationService"/>.
        /// </summary>
        public enum ReplicationAction
        {
            OBTAIN, RELEASE, UPDATE
        }

        /// <summary>
        /// The default context path for the <see cref="ReplicationService"/>.
        /// </summary>
        public const string REPLICATION_CONTEXT = "/replicate";

        /// <summary>
        /// Request parameter name for providing the revision version.
        /// </summary>
        public const string REPLICATE_VERSION_PARAM = "version";

        /// <summary>
        /// Request parameter name for providing a session ID.
        /// </summary>
        public const string REPLICATE_SESSION_ID_PARAM = "sessionid";

        /// <summary>
        /// Request parameter name for providing the file's source.
        /// </summary>
        public const string REPLICATE_SOURCE_PARAM = "source";

        /// <summary>
        /// Request parameter name for providing the file's name.
        /// </summary>
        public const string REPLICATE_FILENAME_PARAM = "filename";

        /// <summary>
        /// Json Serializer Settings to use when serializing and deserializing errors.
        /// </summary>
        public static readonly JsonSerializerSettings JSON_SERIALIZER_SETTINGS = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.All
        };

        private const int SHARD_IDX = 0, ACTION_IDX = 1;

        private readonly string context;
        private readonly IDictionary<string, IReplicator> replicators;

        public ReplicationService(IDictionary<string, IReplicator> replicators, string context = REPLICATION_CONTEXT)
        {
            this.context = context;
            this.replicators = replicators;
        }

        /// <summary>
        /// Returns the path elements that were given in the servlet request, excluding the servlet's action context.
        /// </summary>
        private string[] GetPathElements(IReplicationRequest request)
        {
            string path = request.Path;

            int actionLength = context.Length;
            int startIndex = actionLength;

            if (path.Length > actionLength && path[actionLength] == '/')
                ++startIndex;

            return path.Substring(startIndex).Split('/');
        }

        private static string ExtractRequestParam(IReplicationRequest request, string paramName)
        {
            string param = request.Parameter(paramName);
            if (param == null)
            {
                throw new InvalidOperationException("Missing mandatory parameter: " + paramName);
            }
            return param;
        }


        /// <summary>
        /// Executes the replication task.
        /// </summary>
        /// <exception cref="InvalidOperationException">required parameters are missing</exception>
        public virtual void Perform(IReplicationRequest request, IReplicationResponse response)
        {
            string[] pathElements = GetPathElements(request);
            if (pathElements.Length != 2)
            {
                throw new InvalidOperationException("invalid path, must contain shard ID and action, e.g. */s1/update");
            }

            ReplicationAction action;
            if (!Enum.TryParse(pathElements[ACTION_IDX], true, out action))
            {
                throw new InvalidOperationException("Unsupported action provided: " + pathElements[ACTION_IDX]);
            }

            IReplicator replicator;
            if (!replicators.TryGetValue(pathElements[SHARD_IDX], out replicator))
            {
                throw new InvalidOperationException("unrecognized shard ID " + pathElements[SHARD_IDX]);
            }

            // SOLR-8933 Don't close this stream.
            try
            {
                switch (action)
                {
                    case ReplicationAction.OBTAIN:
                        string sessionId = ExtractRequestParam(request, REPLICATE_SESSION_ID_PARAM);
                        string fileName = ExtractRequestParam(request, REPLICATE_FILENAME_PARAM);
                        string source = ExtractRequestParam(request, REPLICATE_SOURCE_PARAM);
                        using (Stream stream = replicator.ObtainFile(sessionId, source, fileName))
                            stream.CopyTo(response.Body);
                        break;

                    case ReplicationAction.RELEASE:
                        replicator.Release(ExtractRequestParam(request, REPLICATE_SESSION_ID_PARAM));
                        break;

                    case ReplicationAction.UPDATE:
                        string currentVersion = request.Parameter(REPLICATE_VERSION_PARAM);
                        SessionToken token = replicator.CheckForUpdate(currentVersion);
                        if (token == null)
                        {
                            response.Body.Write(new byte[] { 0 }, 0, 1); // marker for null token
                        }
                        else
                        {
                            response.Body.Write(new byte[] { 1 }, 0, 1);
                            token.Serialize(new DataOutputStream(response.Body));
                        }
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            catch (Exception e)
            {
                response.StatusCode = 500;
                try
                {
                    TextWriter writer = new StreamWriter(response.Body);
                    JsonSerializer serializer = JsonSerializer.Create(JSON_SERIALIZER_SETTINGS);
                    serializer.Serialize(writer, e, e.GetType());
                }
                catch (Exception exception)
                {
                    throw new IOException("Could not serialize", exception);
                }
            }
            finally
            {
                response.Flush();
            }
        }

    }
}