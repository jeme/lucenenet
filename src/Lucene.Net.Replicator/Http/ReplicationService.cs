﻿//STATUS: DRAFT - 4.8.0

using System;
using System.Collections.Generic;
using System.IO;
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
    ///   <li>{@code context} is the servlet context, e.g. {@link #REPLICATION_CONTEXT}</li>
    ///   <li>{@code shard} is the ID of the shard, e.g. "s1"</li>
    ///   <li>{@code action} is one of {@link ReplicationAction} values</li>
    /// </ul>
    /// For example, to check whether there are revision updates for shard "s1" you
    /// should send the request: <code>http://host:port/replicate/s1/update</code>.
    /// <p>
    /// This service is written like a servlet, and
    /// {@link #perform(HttpServletRequest, HttpServletResponse)} takes servlet
    /// request and response accordingly, so it is quite easy to embed in your
    /// application's servlet.
    /// </summary>
    /// <remarks>
    /// Lucene.Experimental
    /// </remarks>
    public class ReplicationService
    {
        /** Actions supported by the {@link ReplicationService}. */
        public enum ReplicationAction
        {
            OBTAIN, RELEASE, UPDATE
        }

        /** The context path for the servlet. */
        public const string REPLICATION_CONTEXT = "/replicate";

        /** Request parameter name for providing the revision version. */
        public const string REPLICATE_VERSION_PARAM = "version";

        /** Request parameter name for providing a session ID. */
        public const string REPLICATE_SESSION_ID_PARAM = "sessionid";

        /** Request parameter name for providing the file's source. */
        public const string REPLICATE_SOURCE_PARAM = "source";

        /** Request parameter name for providing the file's name. */
        public const string REPLICATE_FILENAME_PARAM = "filename";

        public static readonly JsonSerializerSettings JSON_SERIALIZER_SETTINGS = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.All
        };

        private const int SHARD_IDX = 0, ACTION_IDX = 1;

        private readonly IDictionary<string, IReplicator> replicators;

        public ReplicationService(IDictionary<String, IReplicator> replicators)
        {
            this.replicators = replicators;
        }

        /**
         * Returns the path elements that were given in the servlet request, excluding
         * the servlet's action context.
         */
        private string[] GetPathElements(IReplicationRequest request)
        {
            string path = request.Path; //request.PathBase + request.Path;
            //TODO: Figure out which parts we need under AspNetCore.
            //string pathInfo = "";//request.Path.Value;
            //TODO: Resharper claims this is always true, if that is the true case then we can just concat them.
            //if (pathInfo != null)
            //    path += pathInfo;

            int actionLength = REPLICATION_CONTEXT.Length;
            int startIndex = actionLength;

            if (path.Length > actionLength && path[actionLength] == '/')
                ++startIndex;

            return path.Substring(startIndex).Split('/');
        }

        private static string ExtractRequestParam(IReplicationRequest request, string paramName) //throws ServletException
        {
            if (!request.ContainsQueryParam(paramName))
            {
                //throw new ServletException("Missing mandatory parameter: " + paramName);
                throw new InvalidOperationException("Missing mandatory parameter: " + paramName);
            }
            //Note: Will throw an exception if the same parameter is provided multiple times.
            return request.QueryParam(paramName);
        }


        /** Executes the replication task. */
        public void Perform(IReplicationRequest request, IReplicationResponse response) //throws ServletException, IOException {
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
                        string currentVersion = request.QueryParam(REPLICATE_VERSION_PARAM);
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
                    JsonSerializer serializer = JsonSerializer.Create(ReplicationService.JSON_SERIALIZER_SETTINGS);

                    serializer.Serialize(writer, e, e.GetType());


                    //BinaryFormatter formatter = new BinaryFormatter();
                    //formatter.Serialize(response.Body, e);
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