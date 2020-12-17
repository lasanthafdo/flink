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
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IterableUtils;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A scheduling agent that will run periodically to reschedule.
 */
public class AdaptiveSchedulingAgent extends AbstractSchedulingAgent {

	private final SchedulingStrategy schedulingStrategy;
	private final NeuralNetworksBasedActorCriticModel actorCriticModel;

	private CompletableFuture<Collection<Acknowledge>> previousRescheduleFuture;

	private final Map<List<Integer>, Double> potentialPlacementActions;
	private final ScheduledExecutorService actorCriticExecutor;
	private final int updatePeriodInSeconds;

	public AdaptiveSchedulingAgent(
		Logger log,
		ExecutionGraph executionGraph,
		SchedulingStrategy schedulingStrategy,
		ScheduledExecutorService actorCriticExecutor,
		long triggerPeriod,
		long waitTimeout,
		int numRetries,
		int updatePeriodInSeconds,
		NeuralNetworkConfiguration neuralNetworkConfiguration) {

		super(log, triggerPeriod, executionGraph, waitTimeout, numRetries);
		this.schedulingStrategy = checkNotNull(schedulingStrategy);
		this.schedulingStrategy.setTopLevelContainer(getTopLevelContainer());

		int nCpus = cpuLayout.cpus();
		int numInputs = (nVertices + 1) * nCpus + edgeMap.size();
		neuralNetworkConfiguration.setNumInputs(numInputs);
		this.actorCriticModel = new NeuralNetworksBasedActorCriticModel(
			nCpus, nVertices, neuralNetworkConfiguration, log);
		this.actorCriticExecutor = actorCriticExecutor;
		this.updatePeriodInSeconds = updatePeriodInSeconds;
		this.potentialPlacementActions = new TopKMap<>(20);
		setupUpdateTriggerThread();
	}

	private void setupUpdateTriggerThread() {
		updateExecutor = actorCriticExecutor.scheduleAtFixedRate(() -> {
			try {
				updateStateInformation();
				updateCurrentPlacementActionInformation();
				if (!currentPlacementAction.isEmpty()) {
					actorCriticModel.updateTrainingData(
						currentPlacementAction,
						new ArrayList<>(getCpuMetrics().values()),
						new ArrayList<>(getEdgeThroughput().values()),
						getOverallThroughput());
				}
				updatePlacementSolution();
			} catch (Exception e) {
				log.error(
					"Encountered exception when trying to update training data: {}",
					e.getMessage(),
					e);
			}
		}, updatePeriodInSeconds, updatePeriodInSeconds, TimeUnit.SECONDS);
	}

	@Override
	public List<Integer> getPlacementSolution() {
		return suggestedPlacementAction;
	}

	@Override
	protected void updatePlacementSolution() {
		double currentThroughput = getOverallThroughput();
		potentialPlacementActions.put(currentPlacementAction, currentThroughput);
		potentialPlacementActions.put(getTrafficBasedPlacementAction(), currentThroughput);

		suggestedPlacementAction = actorCriticModel.selectAction(
			potentialPlacementActions,
			currentThroughput,
			new ArrayList<>(getCpuMetrics().values()),
			new ArrayList<>(edgeFlowRates.values()));
		if (suggestedPlacementAction == null || suggestedPlacementAction.isEmpty()) {
			suggestedPlacementAction = getTrafficBasedPlacementAction();
		}
		if (!isValidPlacementAction(suggestedPlacementAction)) {
			throw new FlinkRuntimeException(
				"Invalid placement action " + suggestedPlacementAction + " suggested.");
		}
		log.info(
			"Suggested placement action: {}, current placement action: {}, throughput: {}",
			suggestedPlacementAction,
			currentPlacementAction,
			currentThroughput);
	}

	@Override
	public void run() {
		if (previousRescheduleFuture == null || previousRescheduleFuture.isDone()) {
			try {
				if (suggestedPlacementAction.equals(currentPlacementAction)) {
					log.info(
						"Current placement action {} is the same as the suggested placement action {}. Skipping rescheduling.",
						currentPlacementAction,
						suggestedPlacementAction);
				} else {
					log.info("Rescheduling job '" + executionGraph.getJobName() + "'");
					previousRescheduleFuture = rescheduleEager();
				}
			} catch (Exception e) {
				log.error(
					"Encountered exception while trying to reschedule : {}",
					e.getMessage(),
					e);
			}
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
