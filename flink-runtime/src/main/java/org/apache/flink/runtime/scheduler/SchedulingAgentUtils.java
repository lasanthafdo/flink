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
