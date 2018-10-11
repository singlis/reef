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

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Common.Jar;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Common
{
    /// <summary>
    /// Helps prepare the driver folder.
    /// </summary>
    internal sealed class DriverFolderPreparationHelper
    {
        private readonly string EvaluatorExecutableConfig = "Org.Apache.REEF.Evaluator.exe.config";
        private static readonly Logger Logger = Logger.GetLogger(typeof(DriverFolderPreparationHelper));
        private readonly AvroConfigurationSerializer _configurationSerializer;
        private readonly REEFFileNames _fileNames;
        private readonly FileSets _fileSets;
        private readonly ISet<IConfigurationProvider> _driverConfigurationProviders;

        /// <summary>
        /// The folder in which we search for the client jar.
        /// In the manner of JavaClientLauncher.cs.
        /// </summary>
        private const string JarFolder = "./";

        [Inject]
        internal DriverFolderPreparationHelper(
            REEFFileNames fileNames,
            AvroConfigurationSerializer configurationSerializer,
            FileSets fileSets,
            [Parameter(typeof(EnvironmentDriverConfigurationProviders))] ISet<IConfigurationProvider> driverConfigurationProviders)
        {
            _fileNames = fileNames;
            _configurationSerializer = configurationSerializer;
            _fileSets = fileSets;
            _driverConfigurationProviders = driverConfigurationProviders;
        }

        /// <summary>
        /// Prepares the working directory for a Driver in driverFolderPath.
        /// </summary>
        /// <param name="appParameters"></param>
        /// <param name="driverFolderPath"></param>
        internal void PrepareDriverFolder(AppParameters appParameters, string driverFolderPath)
        {
            // Add the appParameters into that folder structure
            _fileSets.AddJobFiles(appParameters);

            // Add the reef-bridge-client jar to the local files in the manner of JavaClientLauncher.cs.
            _fileSets.AddToLocalFiles(Directory.GetFiles(JarFolder)
                .Where(file => !string.IsNullOrWhiteSpace(file))
                .Where(jarFile => Path.GetFileName(jarFile).ToLower().StartsWith(ClientConstants.ClientJarFilePrefix)));

            InternalPrepareDriverFolder(appParameters, driverFolderPath);
        }

        /// <summary>
        /// Merges the Configurations in appParameters and serializes them into the right place within driverFolderPath,
        /// assuming
        /// that points to a Driver's working directory.
        /// </summary>
        /// <param name="appParameters"></param>
        /// <param name="driverFolderPath"></param>
        internal void CreateDriverConfiguration(AppParameters appParameters, string driverFolderPath)
        {
            var driverConfigurations = _driverConfigurationProviders.Select(configurationProvider => configurationProvider.GetConfiguration()).ToList();
            var driverConfiguration = Configurations.Merge(driverConfigurations.Concat(appParameters.DriverConfigurations).ToArray());

            _configurationSerializer.ToFile(driverConfiguration,
                Path.Combine(driverFolderPath, _fileNames.GetClrDriverConfigurationPath()));
        }

        /// <summary>
        /// Creates the driver folder structure in this given folder as the root
        /// </summary>
        /// <param name="appParameters">Job submission information</param>
        /// <param name="driverFolderPath">Driver folder path</param>
        internal void CreateDefaultFolderStructure(AppParameters appParameters, string driverFolderPath)
        {
            Directory.CreateDirectory(Path.Combine(driverFolderPath, _fileNames.GetReefFolderName()));
            Directory.CreateDirectory(Path.Combine(driverFolderPath, _fileNames.GetLocalFolderPath()));
            Directory.CreateDirectory(Path.Combine(driverFolderPath, _fileNames.GetGlobalFolderPath()));

            var resourceHelper = new ResourceHelper(typeof(DriverFolderPreparationHelper).Assembly);
            foreach (var fileResources in ResourceHelper.FileResources)
            {
                var fileName = resourceHelper.GetString(fileResources.Key);
                if (ResourceHelper.ClrDriverFullName == fileResources.Key)
                {
                    fileName = Path.Combine(driverFolderPath, _fileNames.GetBridgeExePath());
                }
                else if (ResourceHelper.ClrDriverConfig == fileResources.Key)
                {
                    if (!string.IsNullOrEmpty(appParameters.DriverConfigurationFileContents))
                    {
                        File.WriteAllText(Path.Combine(driverFolderPath, _fileNames.GetBridgeExeConfigPath()),
                                            appParameters.DriverConfigurationFileContents);
                        continue;
                    }

                    fileName = Path.Combine(driverFolderPath, _fileNames.GetBridgeExeConfigPath());
                }
                else if (ResourceHelper.EvaluatorConfig == fileResources.Key)
                {
                    var userDefinedEvaluatorConfigFileName = Path.Combine(JarFolder, EvaluatorExecutableConfig);
                    if (File.Exists(userDefinedEvaluatorConfigFileName))
                    {
                        // Nothing to extract as the user has provided a config
                        continue;
                    }

                    fileName = Path.Combine(driverFolderPath, _fileNames.GetGlobalFolderPath(), EvaluatorExecutableConfig);
                }


                if (!File.Exists(fileName))
                {
                    File.WriteAllBytes(fileName, resourceHelper.GetBytes(fileResources.Value));
                }
            }
        }

        private void InternalPrepareDriverFolder(AppParameters appParameters, string driverFolderPath)
        {
            Logger.Log(Level.Info, "Preparing Driver filesystem layout in {0}", driverFolderPath);

            // Setup the folder structure
            CreateDefaultFolderStructure(appParameters, driverFolderPath);

            // Create the driver configuration
            CreateDriverConfiguration(appParameters, driverFolderPath);

            // Initiate the final copy
            _fileSets.CopyToDriverFolder(driverFolderPath);

            Logger.Log(Level.Info, "Done preparing Driver filesystem layout in {0}", driverFolderPath);
        }
    }
}
