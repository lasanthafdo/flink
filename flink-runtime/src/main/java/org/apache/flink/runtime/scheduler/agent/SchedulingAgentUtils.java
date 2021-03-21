/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.agent;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;

import org.slf4j.Logger;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Utility class to help the scheduling agent.
 */
public class SchedulingAgentUtils {
	public static SchedulingAgent buildSchedulingAgent(
		Logger log,
		ExecutionGraph executionGraph,
		ScheduleMode scheduleMode,
		SchedulingStrategy schedulingStrategy,
		Configuration jobMasterConfiguration,
		SlotPool slotPool,
		ScheduledExecutorService executorService) {

		int nDefaultConfigElements = 4;
		switch (scheduleMode) {
			case PINNED:
				if (jobMasterConfiguration.contains(DeploymentOptions.SCHEDULING_AGENT_CONFIG_STRING)) {
					String agentConfigString = jobMasterConfiguration.getString(
						DeploymentOptions.SCHEDULING_AGENT_CONFIG_STRING);
					String[] configElements = agentConfigString.split(",", 3);
					if (configElements.length == 3) {
						long triggerPeriod = Long.parseLong(configElements[0]);
						long waitTimeOut = Long.parseLong(configElements[1]);
						int numRetries = Integer.parseInt(configElements[2]);

						return new PinnedTaskSchedulingAgent(
							log,
							executionGraph,
							schedulingStrategy,
							triggerPeriod,
							waitTimeOut,
							numRetries);
					} else {
						throw new IllegalConfigurationException(
							"Incorrect number of arguments in the scheduling agent configuration string.");
					}
				} else {
					throw new IllegalConfigurationException(
						"Unable to obtain agent configuration details");
				}
			case TRAFFIC_BASED:
				if (jobMasterConfiguration.contains(DeploymentOptions.SCHEDULING_AGENT_CONFIG_STRING)) {
					String agentConfigString = jobMasterConfiguration.getString(
						DeploymentOptions.SCHEDULING_AGENT_CONFIG_STRING);
					String[] configElements = agentConfigString.split(",", nDefaultConfigElements);
					if (configElements.length == nDefaultConfigElements) {
						long triggerPeriod = Long.parseLong(configElements[0]);
						long waitTimeOut = Long.parseLong(configElements[1]);
						int numRetries = Integer.parseInt(configElements[2]);
						int updatePeriod = Integer.parseInt(configElements[3]);

						return new TrafficBasedSchedulingAgent(
							log,
							executionGraph,
							schedulingStrategy,
							slotPool,
							executorService,
							triggerPeriod,
							waitTimeOut,
							numRetries,
							updatePeriod);
					} else {
						throw new IllegalConfigurationException(
							"Incorrect number of arguments in the scheduling agent configuration string.");
					}
				} else {
					throw new IllegalConfigurationException(
						"Unable to obtain agent configuration details");
				}
			case DRL:
				if (jobMasterConfiguration.contains(DeploymentOptions.SCHEDULING_AGENT_CONFIG_STRING)) {
					String agentConfigString = jobMasterConfiguration.getString(
						DeploymentOptions.SCHEDULING_AGENT_CONFIG_STRING);
					String[] configElements = agentConfigString.split(",", nDefaultConfigElements);
					if (configElements.length == nDefaultConfigElements) {
						long triggerPeriod = Long.parseLong(configElements[0]);
						long waitTimeOut = Long.parseLong(configElements[1]);
						int numRetries = Integer.parseInt(configElements[2]);
						int updatePeriod = Integer.parseInt(configElements[3]);

						return new QActorCriticSchedulingAgent(
							log,
							executionGraph,
							schedulingStrategy,
							slotPool,
							executorService,
							triggerPeriod,
							waitTimeOut,
							numRetries,
							updatePeriod);
					} else {
						throw new IllegalConfigurationException(
							"Incorrect number of arguments in the scheduling agent configuration string.");
					}
				} else {
					throw new IllegalConfigurationException(
						"Unable to obtain agent configuration details");
				}
			case EAGER:
			case LAZY_FROM_SOURCES:
			case LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST:
			default:
				return null;
		}
	}
}
