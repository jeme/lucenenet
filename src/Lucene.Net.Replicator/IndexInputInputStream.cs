﻿//STATUS: INPROGRESS - 4.8.0

using System;
using System.IO;
using Lucene.Net.Store;

namespace Lucene.Net.Replicator
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
    /// 
    /// </summary>
    /// <remarks>
    /// Lucene.Experimental
    /// </remarks>
    public class IndexInputStream : Stream
    {
        private readonly IndexInput input;
        private long remaining;

        public IndexInputStream(IndexInput input)
        {
            this.input = input;
            remaining = input.Length;
        }

        public override void Flush()
        {
            throw new InvalidOperationException("Cannot flush a readonly stream.");
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    Position = offset;
                    break;
                case SeekOrigin.Current:
                    Position += offset;
                    break;
                case SeekOrigin.End:
                    Position = Length - offset;
                    break;
            }
            return Position;
        }

        public override void SetLength(long value)
        {
            throw new InvalidOperationException("Cannot change length of a readonly stream.");
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int remaining = (int) (input.Length - input.GetFilePointer());
            input.ReadBytes(buffer, offset, Math.Min(remaining, count));
            return remaining;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new InvalidCastException("Cannot write to a readonly stream.");
        }

        public override bool CanRead { get { return true; } }
        public override bool CanSeek { get { return true; } }
        public override bool CanWrite { get { return false; } }
        public override long Length { get { return input.Length; } }

        public override long Position
        {
            get { return input.GetFilePointer(); }
            set { input.Seek(value); }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                input.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}