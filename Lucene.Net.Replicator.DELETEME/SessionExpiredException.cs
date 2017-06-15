using System;

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
	/// Exception indicating that a revision update session was expired due to lack
	/// of activity.
	/// </summary>
	/// <seealso cref= LocalReplicator#DEFAULT_SESSION_EXPIRATION_THRESHOLD </seealso>
	/// <seealso cref= LocalReplicator#setExpirationThreshold(long)
	/// 
	/// @lucene.experimental </seealso>
	public class SessionExpiredException : IOException
	{

	  /// <seealso cref= IOException#IOException(String, Throwable) </seealso>
	  public SessionExpiredException(string message, Exception cause) : base(message, cause)
	  {
	  }

	  /// <seealso cref= IOException#IOException(String) </seealso>
	  public SessionExpiredException(string message) : base(message)
	  {
	  }

	  /// <seealso cref= IOException#IOException(Throwable) </seealso>
	  public SessionExpiredException(Exception cause) : base(cause)
	  {
	  }

	}

}