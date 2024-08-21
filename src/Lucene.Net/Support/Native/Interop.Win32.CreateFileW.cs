using System;
using System.Runtime.InteropServices;

namespace Lucene.Net.Native
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

    internal static partial class Interop
    {
        internal static partial class Win32
        {
            // https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew
            [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
            internal static extern IntPtr CreateFileW(
                string lpFileName,
                uint dwDesiredAccess,
                uint dwShareMode,
                IntPtr lpSecurityAttributes,
                uint dwCreationDisposition,
                uint dwFlagsAndAttributes,
                IntPtr hTemplateFile
            );
        }
    }
}
