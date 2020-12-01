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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;

import org.slf4j.Logger;

/**
 * Utility class to help the scheduling agent.
 */
public class SchedulingAgentUtils {
	static PeriodicSchedulingAgent buildSchedulingAgent(Logger log,
														ExecutionGraph executionGraph,
														SchedulingStrategy schedulingStrategy,
														Configuration jobMasterConfiguration) {
		if (jobMasterConfiguration.contains(DeploymentOptions.SCHEDULING_AGENT_CONFIG_STRING)) {
			String agentConfigString = jobMasterConfiguration.getString(
				DeploymentOptions.SCHEDULING_AGENT_CONFIG_STRING);
			String[] configElements = agentConfigString.split(",", 3);
			if (configElements.length == 3) {
				long triggerPeriod = Long.parseLong(configElements[0]);
				long waitTimeOut = Long.parseLong(configElements[1]);
				int numRetries = Integer.parseInt(configElements[2]);

				return new PeriodicSchedulingAgent(log, executionGraph, schedulingStrategy, triggerPeriod, waitTimeOut, numRetries);
			} else {
				throw new IllegalConfigurationException(
					"Incorrect number of arguments in the scheduling agent configuration string.");
			}
		} else {
			return null;
		}
	}
}
