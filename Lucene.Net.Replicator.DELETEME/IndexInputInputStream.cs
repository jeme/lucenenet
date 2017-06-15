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
	/// An <seealso cref="InputStream"/> which wraps an <seealso cref="IndexInput"/>.
	/// 
	/// @lucene.experimental
	/// </summary>
	public sealed class IndexInputInputStream : System.IO.Stream
	{

	  private readonly IndexInput @in;

	  private long remaining;

	  public IndexInputInputStream(IndexInput @in)
	  {
		this.@in = @in;
		remaining = @in.length();
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public int read() throws java.io.IOException
	  public override int read()
	  {
		if (remaining == 0)
		{
		  return -1;
		}
		else
		{
		  --remaining;
		  return @in.readByte();
		}
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public int available() throws java.io.IOException
	  public override int available()
	  {
		return (int) @in.length();
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public void close() throws java.io.IOException
	  public override void close()
	  {
		@in.close();
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public int read(byte[] b) throws java.io.IOException
	  public override int read(sbyte[] b)
	  {
		return read(b, 0, b.Length);
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public int read(byte[] b, int off, int len) throws java.io.IOException
	  public override int read(sbyte[] b, int off, int len)
	  {
		if (remaining == 0)
		{
		  return -1;
		}
		if (remaining < len)
		{
		  len = (int) remaining;
		}
		@in.readBytes(b, off, len);
		remaining -= len;
		return len;
	  }

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public long skip(long n) throws java.io.IOException
	  public override long skip(long n)
	  {
		if (remaining == 0)
		{
		  return -1;
		}
		if (remaining < n)
		{
		  n = remaining;
		}
		@in.seek(@in.FilePointer + n);
		remaining -= n;
		return n;
	  }

	}
}