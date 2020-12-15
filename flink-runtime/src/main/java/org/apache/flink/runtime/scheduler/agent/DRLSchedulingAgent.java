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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionContainer;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A scheduling agent that will run periodically to reschedule.
 */
public class DRLSchedulingAgent extends AbstractSchedulingAgent {

	private final SchedulingStrategy schedulingStrategy;
	private final ActorCriticWrapper actorCriticWrapper;
	private CompletableFuture<Collection<Acknowledge>> previousRescheduleFuture;
	private final ScheduledExecutorService executorService;
	private final long updatePeriodInSeconds;

	public DRLSchedulingAgent(
		Logger log,
		ExecutionGraph executionGraph,
		SchedulingStrategy schedulingStrategy,
		ScheduledExecutorService executorService,
		long triggerPeriod,
		long waitTimeout,
		int numRetries,
		int updatePeriod) {

		super(log, triggerPeriod, executionGraph, waitTimeout, numRetries);
		this.schedulingStrategy = checkNotNull(schedulingStrategy);

		int nVertices = Math.toIntExact(StreamSupport.stream(executionGraph
			.getSchedulingTopology()
			.getVertices().spliterator(), false).count());
		this.actorCriticWrapper = new ActorCriticWrapper(cpuLayout.cpus(), nVertices, log);
		this.executorService = executorService;
		this.updatePeriodInSeconds = updatePeriod;
		setupUpdateTriggerThread();
	}

	private void setupUpdateTriggerThread() {
		updateExecutor = executorService.scheduleAtFixedRate(() -> {
			try {
				setCurrentPlacementSolution();
			} catch (Exception e) {
				log.error(
					"Encountered exception when trying to update state: {}",
					e.getMessage(),
					e);
			}
		}, updatePeriodInSeconds, updatePeriodInSeconds, TimeUnit.SECONDS);
	}

	@Override
	public List<Integer> getPlacementSolution() {
		return currentPlacementAction;
	}

	@Override
	protected void setCurrentPlacementSolution() {
		updateStateInformation();
		List<Integer> assignedCpuIds = new ArrayList<>(getTopLevelContainer()
			.getCurrentCpuAssignment()
			.values());
		int currentStateId = actorCriticWrapper.getStateFor(assignedCpuIds);
		actorCriticWrapper.updateState(
			getOverallThroughput(),
			currentStateId,
			stateId -> -1.0
				* getTopLevelContainer().getResourceUsage(SchedulingExecutionContainer.CPU));
		int currentAction = actorCriticWrapper.getSuggestedAction(currentStateId);
		currentPlacementAction = actorCriticWrapper.getPlacementSolution(currentAction);
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
				schedulingStrategy.startScheduling(this);
			}
		});
	}
}
