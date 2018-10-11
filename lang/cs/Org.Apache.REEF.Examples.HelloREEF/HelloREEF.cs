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

using System;
using System.Globalization;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.AzureBatch;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN.HDI;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.IO.FileSystem.AzureBlob;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.HelloREEF
{
    /// <summary>
    /// A Tool that submits HelloREEFDriver for execution.
    /// </summary>
    public sealed class HelloREEF
    {
        private const string Local = "local";
        private const string YARN = "yarn";
        private const string YARNRest = "yarnrest";
        private const string HDInsight = "hdi";
        private const string AzureBatch = "azurebatch";
        private readonly IREEFClient _reefClient;

        [Inject]
        private HelloREEF(IREEFClient reefClient)
        {
            _reefClient = reefClient;
        }

        /// <summary>
        /// Runs HelloREEF using the IREEFClient passed into the constructor.
        /// </summary>
        private void Run()
        {
            // The driver configuration contains all the needed bindings.
            var helloDriverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<HelloDriver>.Class)
                .Set(DriverConfiguration.OnDriverStarted, GenericType<HelloDriver>.Class)
                .Set(DriverConfiguration.CustomTraceLevel, Level.Verbose.ToString())
                .Build();

            string applicationId = GetApplicationId();

            // The JobSubmission contains the Driver configuration as well as the files needed on the Driver.
            var helloJobRequest = _reefClient.NewJobRequestBuilder()
                .AddDriverConfiguration(helloDriverConfiguration)
                .AddGlobalAssemblyForType(typeof(HelloDriver))
                .AddGlobalAssembliesInDirectoryOfExecutingAssembly()
                .SetJobIdentifier(applicationId)
                .SetJavaLogLevel(JavaLoggingSetting.Verbose)
                .Build();

            IJobSubmissionResult jobSubmissionResult = _reefClient.SubmitAndGetJobStatus(helloJobRequest);

            // Wait for the Driver to complete.
            jobSubmissionResult?.WaitForDriverToFinish();
        }

        private string GetApplicationId()
        {
            return "HelloWorldJob-" + DateTime.Now.ToString("ddd-MMM-d-HH-mm-ss-yyyy", CultureInfo.CreateSpecificCulture("en-US"));
        }

        /// <summary>
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private static IConfiguration GetRuntimeConfiguration(string name)
        {
            switch (name)
            {
                case Local:
                    return LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, "2")
                        .Build();

                case YARN:
                    return YARNClientConfiguration.ConfigurationModule.Build();

                case YARNRest:
                    return YARNClientConfiguration.ConfigurationModuleYARNRest.Build();

                case HDInsight:
                    // To run against HDInsight please replace placeholders below, with actual values for
                    // blob storage account name and key, container name (available at Azure portal) and HDInsight
                    // credentials (username and password)
                    const string blobStorageAccountName = "name";
                    const string blobStorageAccountKey = "key";
                    const string containerName = "foo";
                    return HDInsightClientConfiguration.ConfigurationModule
                        .Set(HDInsightClientConfiguration.HDInsightPasswordParameter, @"pwd")
                        .Set(HDInsightClientConfiguration.HDInsightUsernameParameter, @"foo")
                        .Set(HDInsightClientConfiguration.HDInsightUrlParameter, @"https://foo.azurehdinsight.net/")
                        .Set(HDInsightClientConfiguration.JobSubmissionDirectoryPrefix, $@"/{containerName}/tmp")
                        .Set(AzureBlobFileSystemConfiguration.AccountName, blobStorageAccountName)
                        .Set(AzureBlobFileSystemConfiguration.AccountKey, blobStorageAccountKey)
                        .Build();

                case AzureBatch:
                    return AzureBatchRuntimeClientConfiguration.ConfigurationModule
                        .Set(AzureBatchRuntimeClientConfiguration.AzureBatchAccountKey, @"zUyBm+pfwbkJmUgXTe9Nw95vRVLKWIp9baR7SdjfuI83CeW3z1UJzds9btO6IHqnRuL6xh1fOsjTp6YhAK5afg==")
                        .Set(AzureBatchRuntimeClientConfiguration.AzureBatchAccountName, @"singlisazb")
                        .Set(AzureBatchRuntimeClientConfiguration.AzureBatchAccountUri, @"https://singlisazb.centralus.batch.azure.com")
                        .Set(AzureBatchRuntimeClientConfiguration.AzureBatchPoolId, @"singlisaz6")
                        .Set(AzureBatchRuntimeClientConfiguration.AzureStorageAccountKey, @"shPss/fn8ibMl1ODdImcS7XTh2gn8m2TWoAQrgWMJoJy24/XJjWxuS9o5bBgNrGD4GPoqrvadqKNAl/NYQFOfQ==")
                        .Set(AzureBatchRuntimeClientConfiguration.AzureStorageAccountName, @"singlisazs")
                        .Set(AzureBatchRuntimeClientConfiguration.AzureStorageContainerName, @"test-run")
                        //// Extend default retry interval in Azure Batch
                        .Set(AzureBatchRuntimeClientConfiguration.DriverHTTPConnectionRetryInterval, "20000")
                        //// Following list of ports is required to enable the following options:
                        //// 1. To enable communication between driver and client:
                        ////        The ports will be used to set up driver http server endpoint.
                        ////        In addition, these ports must also be defined in Azure Batch InBoundNATPool to enable communication.
                        //// 2. To enable communication between docker containers:
                        ////        These ports will be mapped between the container and host to
                        ////        allow the communication between the containers.
                        ////        In addition, the Azure Batch pool must be created as a container pool to use this feature.
                        // .Set(AzureBatchRuntimeClientConfiguration.AzureBatchPoolDriverPortsList, new List<string> { "2000", "2001", "2002" })
                        //// Bind to Container Registry properties if present
                        // .Set(AzureBatchRuntimeClientConfiguration.ContainerRegistryServer, @"###############")
                        // .Set(AzureBatchRuntimeClientConfiguration.ContainerRegistryUsername, @"###############")
                        // .Set(AzureBatchRuntimeClientConfiguration.ContainerRegistryPassword, @"###############")
                        // .Set(AzureBatchRuntimeClientConfiguration.ContainerImageName, @"###############")
                        .Build();

                default:
                    throw new Exception("Unknown runtime: " + name);
            }
        }

        public static void MainSimple(string[] args)
        {
            //var runtime = args.Length > 0 ? args[0] : Local;
            var runtime = AzureBatch;
                //args.Length > 0 ? args[0] : Local;

            // Execute the HelloREEF, with these parameters injected
            TangFactory.GetTang()
                .NewInjector(GetRuntimeConfiguration(runtime))
                .GetInstance<HelloREEF>()
                .Run();
        }
    }
}
