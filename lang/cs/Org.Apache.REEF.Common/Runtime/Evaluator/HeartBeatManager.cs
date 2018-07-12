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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator.Context;
using Org.Apache.REEF.Common.Runtime.Evaluator.Utils;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.Time;
using Org.Apache.REEF.Wake.Time.Event;
using System.Diagnostics;

namespace Org.Apache.REEF.Common.Runtime.Evaluator
{
    [ThreadSafe]
    internal sealed class HeartBeatManager : IHeartBeatManager
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(HeartBeatManager));

        private static readonly MachineStatus MachineStatus = new MachineStatus();

        private readonly IRemoteManager<REEFMessage> _remoteManager;

        private readonly IClock _clock;

        private readonly int _heartBeatPeriodInMillSeconds;

        private readonly int _maxHeartbeatRetries = 0;

        private readonly int _maxHeartbeatRetriesForNonRecoveryMode = 0;

        private IRemoteIdentifier _remoteId;

        private IObserver<REEFMessage> _observer;

        private int _heartbeatFailures = 0;

        private readonly IInjectionFuture<IDriverConnection> _driverConnection;

        private readonly EvaluatorSettings _evaluatorSettings;

        private readonly IInjectionFuture<EvaluatorRuntime> _evaluatorRuntime;

        private readonly IInjectionFuture<ContextManager> _contextManager;

        private bool _isCompletedHeartbeatQueued = false;

        private static readonly Logger Logger = Logger.GetLogger(typeof(HeartBeatManager));

        // the queue can only contains the following:
        // 1. all failed heartbeats (regular and event-based) before entering RECOVERY state
        // 2. event-based heartbeats generated in RECOVERY state (since there will be no attempt to send regular heartbeat)
        private readonly Queue<EvaluatorHeartbeatProto> _queuedHeartbeats = new Queue<EvaluatorHeartbeatProto>();

        [Inject]
        private HeartBeatManager(
            EvaluatorSettings settings,
            IInjectionFuture<EvaluatorRuntime> evaluatorRuntime,
            IInjectionFuture<ContextManager> contextManager,
            [Parameter(typeof(ErrorHandlerRid))] string errorHandlerRid,
            IInjectionFuture<IDriverConnection> driverConnection)
        {
            using (LOGGER.LogFunction("HeartBeatManager::HeartBeatManager"))
            {
                _evaluatorSettings = settings;
                _evaluatorRuntime = evaluatorRuntime;
                _contextManager = contextManager;
                _remoteManager = settings.RemoteManager;
                _remoteId = new SocketRemoteIdentifier(NetUtilities.ParseIpEndpoint(errorHandlerRid));
                _observer = _remoteManager.GetRemoteObserver(new RemoteEventEndPoint<REEFMessage>(_remoteId));
                _clock = settings.RuntimeClock;
                _heartBeatPeriodInMillSeconds = settings.HeartBeatPeriodInMs;
                _maxHeartbeatRetries = settings.MaxHeartbeatRetries;
                _maxHeartbeatRetriesForNonRecoveryMode = settings.MaxHeartbeatRetriesForNonRecoveryMode;
                _driverConnection = driverConnection;
                MachineStatus.ToString(); // kick start the CPU perf counter
            }
        }

        /// <summary>
        /// Return EvaluatorRuntime referenced from HeartBeatManager
        /// </summary>
        public EvaluatorRuntime EvaluatorRuntime
        {
            get { return _evaluatorRuntime.Get(); }
        }

        /// <summary>
        /// Return ContextManager referenced from HeartBeatManager
        /// </summary>
        public ContextManager ContextManager
        {
            get { return _contextManager.Get(); }
        }

        /// <summary>
        /// EvaluatorSettings contains the configuration data of the evaluators
        /// </summary>
        public EvaluatorSettings EvaluatorSettings
        {
            get { return _evaluatorSettings; }
        }

        public void Send(EvaluatorHeartbeatProto evaluatorHeartbeatProto)
        {
            lock (_queuedHeartbeats)
            {
                // Do not send a heartbeat if Evaluator has already signaled that it was done.
                if (_isCompletedHeartbeatQueued)
                {
                    LOGGER.Log(Level.Warning, "Evaluator trying to schedule a heartbeat after a completed heartbeat has already been scheduled or sent.");
                    return;
                }

                if (IsEvaluatorStateCompleted(evaluatorHeartbeatProto.evaluator_status.state))
                {
                    _isCompletedHeartbeatQueued = true;
                }

                if (_evaluatorSettings.OperationState == EvaluatorOperationState.RECOVERY)
                {
                    LOGGER.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "In RECOVERY mode, heartbeat queued as [{0}]. ", evaluatorHeartbeatProto));
                    _queuedHeartbeats.Enqueue(evaluatorHeartbeatProto);
                    return;
                }

                // NOT during recovery, try to send
                REEFMessage payload = new REEFMessage(evaluatorHeartbeatProto);
                try
                {
                    _observer.OnNext(payload);
                    _heartbeatFailures = 0; // reset failure counts if we are having intermittent (not continuous) failures
                }
                catch (Exception e)
                {
                    if (evaluatorHeartbeatProto.task_status == null || evaluatorHeartbeatProto.task_status.state != State.RUNNING)
                    {
                        Utilities.Diagnostics.Exceptions.Throw(e, "Lost communications to driver when no task is running, recovery NOT supported for such scenario", LOGGER);
                    }

                    _heartbeatFailures++;

                    _queuedHeartbeats.Enqueue(evaluatorHeartbeatProto);
                    LOGGER.Log(Level.Error, string.Format(CultureInfo.InvariantCulture, "Sending heartbeat to driver experienced #{0} failure. Hearbeat queued as: [{1}]. ", _heartbeatFailures, evaluatorHeartbeatProto), e);

                    if (_driverConnection.Get() is MissingDriverConnection)
                    {
                        if (_heartbeatFailures >= _maxHeartbeatRetriesForNonRecoveryMode)
                        {
                            var msg =
                                string.Format(CultureInfo.InvariantCulture,
                                    "Have encountered {0} heartbeat failures. Limit of heartbeat sending failures exceeded. Driver reconnect logic is not implemented, failing evaluator.",
                                    _heartbeatFailures);
                            LOGGER.Log(Level.Error, msg);
                            throw new ReefRuntimeException(msg, e);
                        }
                    }
                    else
                    {
                        if (_heartbeatFailures >= _maxHeartbeatRetries)
                        {
                            LOGGER.Log(Level.Warning,
                                "Heartbeat communications to driver reached max of {0} failures. Driver is considered dead/unreachable",
                                _heartbeatFailures);
                            LOGGER.Log(Level.Info, "Entering RECOVERY mode!!!");
                            ContextManager.HandleDriverConnectionMessage(
                                new DriverConnectionMessageImpl(DriverConnectionState.Disconnected));

                            LOGGER.Log(Level.Info, "instantiate driver reconnect implementation: " + _driverConnection);
                            _evaluatorSettings.OperationState = EvaluatorOperationState.RECOVERY;

                            // clean heartbeat failure
                            _heartbeatFailures = 0;
                        }
                    }
                }
            }     
        }

        /// <summary>
        /// Assemble a complete new heartbeat and send it out.
        /// </summary>
        public void OnNext()
        {
            LOGGER.Log(Level.Verbose, "Before acquiring lock: HeartbeatManager::OnNext()");
            lock (this)
            {
                LOGGER.Log(Level.Verbose, "HeartbeatManager::OnNext()");
                EvaluatorHeartbeatProto heartbeatProto = GetEvaluatorHeartbeatProto();
                LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Triggered a heartbeat: {0}.", heartbeatProto));
                Send(heartbeatProto);
            }
        }

        /// <summary>
        /// Called with a specific TaskStatus that must be delivered to the driver
        /// </summary>
        /// <param name="taskStatusProto"></param>
        public void OnNext(TaskStatusProto taskStatusProto)
        {
            LOGGER.Log(Level.Verbose, "Before acquiring lock: HeartbeatManager::OnNext(TaskStatusProto)");
            lock (this)
            {
                LOGGER.Log(Level.Verbose, "HeartbeatManager::OnNext(TaskStatusProto)");
                EvaluatorHeartbeatProto heartbeatProto = GetEvaluatorHeartbeatProto(
                    EvaluatorRuntime.GetEvaluatorStatus(),
                    ContextManager.GetContextStatusCollection(),
                     Optional<TaskStatusProto>.Of(taskStatusProto));
                LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Triggered a heartbeat: {0}.", heartbeatProto));
                Send(heartbeatProto);
            }
        }

        /// <summary>
        ///  Called with a specific ContextStatusProto that must be delivered to the driver
        /// </summary>
        /// <param name="contextStatusProto"></param>
        public void OnNext(ContextStatusProto contextStatusProto)
        {
            LOGGER.Log(Level.Verbose, "Before acquiring lock: HeartbeatManager::OnNext(ContextStatusProto)");
            lock (this)
            {
                LOGGER.Log(Level.Verbose, "HeartbeatManager::OnNext(ContextStatusProto)");
                List<ContextStatusProto> contextStatusProtos = new List<ContextStatusProto>();
                contextStatusProtos.Add(contextStatusProto);
                contextStatusProtos.AddRange(ContextManager.GetContextStatusCollection());
                EvaluatorHeartbeatProto heartbeatProto = GetEvaluatorHeartbeatProto(
                    EvaluatorRuntime.GetEvaluatorStatus(),
                    contextStatusProtos,
                    Optional<TaskStatusProto>.Empty());
                LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Triggered a heartbeat: {0}.", heartbeatProto));
                Send(heartbeatProto);
            }
        }

        /// <summary>
        /// Called with a specific EvaluatorStatus that must be delivered to the driver
        /// </summary>
        /// <param name="evaluatorStatusProto"></param>
        public void OnNext(EvaluatorStatusProto evaluatorStatusProto)
        {
            LOGGER.Log(Level.Verbose, "Before acquiring lock: HeartbeatManager::OnNext(EvaluatorStatusProto)");
            lock (this)
            {
                LOGGER.Log(Level.Verbose, "HeartbeatManager::OnNext(EvaluatorStatusProto)");
                EvaluatorHeartbeatProto heartbeatProto = new EvaluatorHeartbeatProto()
                {
                    timestamp = CurrentTimeMilliSeconds(),
                    evaluator_status = evaluatorStatusProto
                };
                LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Triggered a heartbeat: {0}.", heartbeatProto));
                Send(heartbeatProto);
            }
        }

        public void OnNext(Alarm value)
        {
            LOGGER.Log(Level.Verbose, "Before acquiring lock: HeartbeatManager::OnNext(Alarm)");
            lock (this)
            {
                LOGGER.Log(Level.Verbose, "HeartbeatManager::OnNext(Alarm)");
                
                if (_evaluatorSettings.OperationState == EvaluatorOperationState.OPERATIONAL && EvaluatorRuntime.State == State.RUNNING)
                {
                    try
                    {
                        EvaluatorHeartbeatProto evaluatorHeartbeatProto = GetEvaluatorHeartbeatProto();
                        LOGGER.Log(Level.Verbose,
                            string.Format(CultureInfo.InvariantCulture, "Triggered a heartbeat: {0}. {1}Node Health: {2}", evaluatorHeartbeatProto, Environment.NewLine, MachineStatus));
                        Send(evaluatorHeartbeatProto);
                    }
                    catch (Exception e)
                    {
                        Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                        EvaluatorRuntime.OnException(e);
                    }
                }
                else
                {
                    LOGGER.Log(Level.Verbose, "Ignoring regular heartbeat since Evaluator operation state is [{0}] and runtime state is [{1}]. ", EvaluatorSettings.OperationState, EvaluatorRuntime.State);

                    // Do not try to recover if Evaluator is done.
                    if (IsEvaluatorStateCompleted(EvaluatorRuntime.State))
                    {
                        return;
                    }

                    if (_evaluatorSettings.OperationState == EvaluatorOperationState.RECOVERY)
                    {
                        var driverConnection = _driverConnection.Get();
                        try
                        {
                            var driverInformation = driverConnection.GetDriverInformation();
                            if (driverInformation == null)
                            {
                                LOGGER.Log(Level.Verbose,
                                    "In RECOVERY mode, cannot retrieve driver information, will try again later.");
                            }
                            else
                            {
                                var msg = string.Format(CultureInfo.InvariantCulture,
                                        "Detect driver restarted at {0} and is running on endpoint {1} with services {2}. Now trying to re-establish connection",
                                        driverInformation.DriverStartTime,
                                        driverInformation.DriverRemoteIdentifier,
                                        driverInformation.NameServerId);
                                LOGGER.Log(Level.Info, msg);
                                Recover(driverInformation);
                            }
                        }
                        catch (NotImplementedException)
                        {
                            LOGGER.Log(Level.Error, "Reaching EvaluatorOperation RECOVERY mode, however, there is no IDriverConnection implemented for HA.");
                            throw;
                        }
                        catch (Exception e)
                        {
                            // we do not want any exception to stop the query for driver status
                            Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, LOGGER);
                        }
                    }
                }

                _clock.ScheduleAlarm(_heartBeatPeriodInMillSeconds, this);
            }
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        private static bool IsEvaluatorStateCompleted(State state)
        {
            return state == State.DONE || state == State.FAILED || state == State.KILLED;
        }

        private static long CurrentTimeMilliSeconds()
        {
            // this is an implementation to get current time in milliseconds counted from Jan 1st, 1970
            // it is chosen as such to be compatible with Java implementation
            DateTime jan1St1970 = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            return (long)(DateTime.UtcNow - jan1St1970).TotalMilliseconds;
        }

        private void Recover(DriverInformation driverInformation)
        {
            IPEndPoint driverEndpoint = NetUtilities.ParseIpEndpoint(driverInformation.DriverRemoteIdentifier);
            _remoteId = new SocketRemoteIdentifier(driverEndpoint);
            _observer = _remoteManager.GetRemoteObserver(new RemoteEventEndPoint<REEFMessage>(_remoteId));
            lock (_evaluatorSettings)
            {
                if (_evaluatorSettings.NameClient != null)
                {
                    try
                    {
                        LOGGER.Log(Level.Verbose, "Trying to reset and reconnect to name server" + driverInformation.NameServerId);
                        _evaluatorSettings.NameClient.Restart(NetUtilities.ParseIpEndpoint(driverInformation.NameServerId));
                        LOGGER.Log(Level.Info, "Reconnected to name server: " + driverInformation.NameServerId);
                    }
                    catch (Exception e)
                    {
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                    }
                }
            }

            lock (_queuedHeartbeats)
            {
                bool firstHeartbeatInQueue = true;
                while (_queuedHeartbeats.Any())
                {
                    LOGGER.Log(Level.Info, "Sending cached recovery heartbeats to " + _remoteId);
                    try
                    {
                        if (firstHeartbeatInQueue)
                        {
                            // first heartbeat is specially constructed to include the recovery flag
                            EvaluatorHeartbeatProto recoveryHeartbeat = ConstructRecoveryHeartBeat(_queuedHeartbeats.Dequeue());
                            LOGGER.Log(Level.Info, "Recovery heartbeat to be sent:" + recoveryHeartbeat);
                            _observer.OnNext(new REEFMessage(recoveryHeartbeat));
                            firstHeartbeatInQueue = false;
                        }
                        else
                        {
                            _observer.OnNext(new REEFMessage(_queuedHeartbeats.Dequeue()));
                        }
                    }
                    catch (Exception e)
                    {
                        // we do not handle failures during RECOVERY 
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.CaughtAndThrow(
                            e,
                            Level.Error,
                            string.Format(CultureInfo.InvariantCulture, "Hearbeat attempt failed in RECOVERY mode to Driver {0} , giving up...", _remoteId),
                            LOGGER);
                    }
                    Thread.Sleep(500);
                }
            }

            _evaluatorSettings.OperationState = EvaluatorOperationState.OPERATIONAL;
            ContextManager.HandleDriverConnectionMessage(new DriverConnectionMessageImpl(DriverConnectionState.Reconnected));

            LOGGER.Log(Level.Info, "Exiting RECOVERY mode!!!");
        }

        private EvaluatorHeartbeatProto ConstructRecoveryHeartBeat(EvaluatorHeartbeatProto heartbeat)
        {
            heartbeat.recovery = true;
            heartbeat.context_status.ForEach(c => c.recovery = true);
            heartbeat.task_status.recovery = true;
            return heartbeat;
        }

        private EvaluatorHeartbeatProto GetEvaluatorHeartbeatProto()
        {
            return GetEvaluatorHeartbeatProto(
                EvaluatorRuntime.GetEvaluatorStatus(),
                ContextManager.GetContextStatusCollection(),
                ContextManager.GetTaskStatus());
        }

        private EvaluatorHeartbeatProto GetEvaluatorHeartbeatProto(
            EvaluatorStatusProto evaluatorStatusProto,
            ICollection<ContextStatusProto> contextStatusProtos,
            Optional<TaskStatusProto> taskStatusProto)
        {
            EvaluatorHeartbeatProto evaluatorHeartbeatProto = new EvaluatorHeartbeatProto()
            {
                timestamp = CurrentTimeMilliSeconds(),
                evaluator_status = evaluatorStatusProto
            };
            foreach (ContextStatusProto contextStatusProto in contextStatusProtos)
            {
                evaluatorHeartbeatProto.context_status.Add(contextStatusProto);
            }
            if (taskStatusProto.IsPresent())
            {
                evaluatorHeartbeatProto.task_status = taskStatusProto.Value;
            }
            return evaluatorHeartbeatProto;
        }
    }
}
