package org.apache.flink.runtime.scheduler;

/**
 * The scheduling agent interface used for dynamic oeprator placement.
 */
public interface SchedulingAgent extends Runnable {
	/**
	 * Returns the period between executions for this agent.
	 *
	 * @return the period between executions in milliseconds
	 */
	long getTriggerPeriod();
}
