﻿// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#if REEF_DOTNET_BUILD
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
#else
using Microsoft.Practices.TransientFaultHandling;
#endif

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Wake.Remote
{
    /// <summary>
    /// Interface for the retry logic to connect to remote endpoint
    /// </summary>
    [DefaultImplementation(typeof(RemoteConnectionRetryHandler))]
    public interface IConnectionRetryHandler
    {
        /// <summary>
        /// Retry policy for the tcp connection
        /// </summary>
        RetryPolicy Policy { get; }
    }
}
