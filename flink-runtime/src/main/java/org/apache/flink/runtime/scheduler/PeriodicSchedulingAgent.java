package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.SchedulingUtils;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;

import org.slf4j.Logger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A scheduling agent that will run periodically to reschedule.
 */
public class PeriodicSchedulingAgent implements Runnable {

	private final ExecutionGraph executionGraph;
	private final SchedulingStrategy schedulingStrategy;
	private final Logger log;

	public PeriodicSchedulingAgent(Logger log, ExecutionGraph executionGraph, SchedulingStrategy schedulingStrategy) {
		this.log = log;
		this.executionGraph = checkNotNull(executionGraph);
		this.schedulingStrategy = checkNotNull(schedulingStrategy);
	}

	@Override
	public void run() {
/*
		JobStatus currentState = schedulingStrategy.getState();
		if (currentState != JobStatus.RUNNING) {
			if (schedulingStrategy.transitionState(currentState, JobStatus.RESTARTING)) {
				Iterable<ExecutionVertex> vertices = schedulingStrategy.getAllExecutionVertices();
				List<CompletableFuture<Acknowledge>> allCancelFutures = new ArrayList<>();
				for (ExecutionVertex ev : vertices) {
					CompletableFuture<Acknowledge> cancelFuture = ev.getCurrentExecutionAttempt().haltExecution();
					allCancelFutures.add(cancelFuture);
				}
				FutureUtils.combineAll(allCancelFutures);
				schedulingStrategy.restart(schedulingStrategy.getGlobalModVersion());
			}
		} else {
*/
		log.info("Rescheduling job '" + executionGraph.getJobName() + "'");
		SchedulingUtils.rescheduleEager(executionGraph, schedulingStrategy, log);
	}
}
