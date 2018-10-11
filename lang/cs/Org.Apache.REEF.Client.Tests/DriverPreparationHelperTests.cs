// Licensed to the Apache Software Foundation (ASF) under one
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


using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.IO.TempFileCreation;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using System;
using System.Collections.Generic;
using Xunit;

namespace Org.Apache.REEF.Client.Tests
{
    public sealed class TestDriver : IObserver<IDriverStarted>
    {
        public void OnNext(IDriverStarted driverStarted)
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    };

    public sealed class DriverFolderPreparationHelperTests
    {
        [Fact]
        public void TestDriverPreparation()
        {
           var driverConfig = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TestDriver>.Class)
                .Build();
           var localRuntime = LocalRuntimeClientConfiguration.ConfigurationModule
                .Set(LocalRuntimeClientConfiguration.RuntimeFolder, "foo")
                .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, "2")
                .Build();

           var tempFileConfig = TempFileConfigurationModule.ConfigurationModule.Build();
           var config = Configurations.Merge(localRuntime, driverConfig, tempFileConfig);
           
            var injector = TangFactory.GetTang().NewInjector(config);
            var driverHelper = injector.GetInstance<DriverFolderPreparationHelper>();

            var appParameters = new AppParameters(new HashSet<IConfiguration>() { driverConfig },
                                                  new HashSet<string>() { },
                                                  new HashSet<string>() { }, 
                                                  new HashSet<string>() { }, 
                                                  new HashSet<string>() { }, 
                                                  "");

            driverHelper.PrepareDriverFolder(appParameters, "reef");

/*
            var driverFolderPrepartionHelper = injector.GetInstance<DriverFolderPreparationHelper>(
        REEFFileNames fileNames,
        AvroConfigurationSerializer configurationSerializer,
        FileSets fileSets,
        [Parameter(typeof(EnvironmentDriverConfigurationProviders))] ISet<IConfigurationProvider> driverConfigurationProviders)
                );
            driverFolderPrepartionHelper.CreateDefaultFolderStructure()
            */
        }

    }
}