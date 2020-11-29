package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A scheduling agent that will run periodically to reschedule.
 */
public class PeriodicSchedulingAgent implements Runnable {

	private final ExecutionGraph executionGraph;
	private final SchedulingStrategy schedulingStrategy;
	private final Logger log;
	private final long triggerPeriod;
	private final long waitTimeout;
	private final int numRetries;

	private CompletableFuture<Collection<Acknowledge>> previousRescheduleFuture;

	public PeriodicSchedulingAgent(
		Logger log, ExecutionGraph executionGraph, SchedulingStrategy schedulingStrategy,
		long triggerPeriod, long waitTimeout, int numRetries) {

		this.log = log;
		this.executionGraph = checkNotNull(executionGraph);
		this.schedulingStrategy = checkNotNull(schedulingStrategy);
		this.triggerPeriod = triggerPeriod;
		this.waitTimeout = waitTimeout;
		this.numRetries = numRetries;
	}

	public long getTriggerPeriod() {
		return triggerPeriod;
	}

	@Override
	public void run() {
		if (previousRescheduleFuture == null || previousRescheduleFuture.isDone()) {
			log.info("Rescheduling job '" + executionGraph.getJobName() + "'");
			previousRescheduleFuture = rescheduleEager();
		}
	}

	private CompletableFuture<Collection<Acknowledge>> rescheduleEager() {
		checkState(executionGraph.getState() == JobStatus.RUNNING, "job is not running currently");

		final Iterable<ExecutionVertex> vertices = executionGraph.getAllExecutionVertices();
		final ArrayList<CompletableFuture<Acknowledge>> allHaltFutures = new ArrayList<>();

		for (ExecutionVertex ev : vertices) {
			Execution attempt = ev.getCurrentExecutionAttempt();
			CompletableFuture<Acknowledge> haltFuture = attempt
				.haltExecution()
				.whenCompleteAsync((ack, fail) -> {
					String taskNameWithSubtaskIndex = attempt
						.getVertex()
						.getTaskNameWithSubtaskIndex();
					for (int i = 0; i < numRetries; i++) {
						if (attempt.getState() != ExecutionState.CREATED) {
							try {
								Thread.sleep(waitTimeout);
							} catch (InterruptedException exception) {
								log.warn(
									"Thread waiting on halting of task {} was interrupted due to cause : {}",
									taskNameWithSubtaskIndex,
									exception);
							}
						} else {
							if (log.isDebugEnabled()) {
								log.debug("Task '" + taskNameWithSubtaskIndex
									+ "' changed to expected state (" +
									ExecutionState.CREATED + ") while waiting " + i + " times");
							}
							return;
						}
					}
					log.error("Couldn't halt execution for task {}.", taskNameWithSubtaskIndex);
					FutureUtils.completedExceptionally(new Exception(
						"Couldn't halt execution for task " + taskNameWithSubtaskIndex));
				});
			allHaltFutures.add(haltFuture);
		}
		final FutureUtils.ConjunctFuture<Collection<Acknowledge>> allHaltsFuture = FutureUtils.combineAll(
			allHaltFutures);
		return allHaltsFuture.whenComplete((ack, fail) -> {
			if (fail != null) {
				log.error("Encountered exception when halting process.", fail);
				throw new CompletionException("Halt process unsuccessful", fail);
			} else {
				schedulingStrategy.startScheduling();
			}
		});
	}
}
