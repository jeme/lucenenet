//STATUS: PENDING - 4.8.0
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
    /// Base class for Http clients.
    /// </summary>
    /// <remarks>
    /// Lucene.Experimental
    /// </remarks>
    public class HttpClientBase : IDisposable
    {
        /// <summary>
        /// Default connection timeout for this client, in milliseconds.
        /// </summary>
        /// <seealso cref= #setConnectionTimeout(int) </seealso>
        public const int DEFAULT_CONNECTION_TIMEOUT = 1000;

        /// <summary>
        /// Default socket timeout for this client, in milliseconds.
        /// </summary>
        /// <seealso cref= #setSoTimeout(int) </seealso>
        public const int DEFAULT_SO_TIMEOUT = 60000;

        // TODO compression?
        /// <summary>
        /// The URL stting to execute requests against. 
        /// </summary>
        protected readonly string url;




        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                //TODO:
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
