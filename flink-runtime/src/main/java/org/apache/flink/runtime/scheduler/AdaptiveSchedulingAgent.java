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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.strategy.AbstractSchedulingAgent;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionContainer;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IterableUtils;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A scheduling agent that will run periodically to reschedule.
 */
public class AdaptiveSchedulingAgent extends AbstractSchedulingAgent {

	private final SchedulingStrategy schedulingStrategy;
	private final Logger log;
	private final long waitTimeout;
	private final int numRetries;
	private final NeuralNetworksBasedActorCriticModel actorCriticModel;

	private CompletableFuture<Collection<Acknowledge>> previousRescheduleFuture;

	private final Map<List<Integer>, Double> potentialPlacementActions = new HashMap<>();
	private List<Integer> currentPlacementAction;
	private List<Integer> previousPlacementAction;
	private final ScheduledExecutorService actorCriticExecutor;
	private final int nCpus;
	private final int nVertices;

	public AdaptiveSchedulingAgent(
		Logger log,
		ExecutionGraph executionGraph,
		SchedulingStrategy schedulingStrategy,
		ScheduledExecutorService actorCriticExecutor,
		long triggerPeriod,
		long waitTimeout,
		int numRetries,
		NeuralNetworkConfiguration neuralNetworkConfiguration) {

		super(log, triggerPeriod, executionGraph);
		this.log = log;
		this.schedulingStrategy = checkNotNull(schedulingStrategy);
		this.schedulingStrategy.setTopLevelContainer(getTopLevelContainer());
		this.waitTimeout = waitTimeout;
		this.numRetries = numRetries;

		this.nCpus = cpuLayout.cpus();
		this.nVertices = executionGraph.getTotalNumberOfVertices();
		int numInputs = (nVertices + 1) * nCpus + edgeMap.size();
		neuralNetworkConfiguration.setNumInputs(numInputs);
		this.actorCriticModel = new NeuralNetworksBasedActorCriticModel(
			nCpus, nVertices, neuralNetworkConfiguration, log);
		this.actorCriticExecutor = actorCriticExecutor;
		this.previousPlacementAction = new ArrayList<>();
		setupUpdateTriggerThread();
	}

	private void setupUpdateTriggerThread() {
		actorCriticExecutor.scheduleAtFixedRate(() -> {
			try {
				if (!previousPlacementAction.isEmpty()) {
					actorCriticModel.updateTrainingData(
						previousPlacementAction,
						new ArrayList<>(getCpuMetrics().values()),
						new ArrayList<>(getEdgeThroughput().values()),
						getOverallThroughput());
				}
			} catch (Exception e) {
				log.error(
					"Encountered exception when trying to update training data: {}",
					e.getMessage(),
					e);
			}
		}, 10, 10, TimeUnit.SECONDS);
	}

	@Override
	public List<Integer> getPlacementSolution() {
		return currentPlacementAction;
	}

	@Override
	public void run() {
		if (previousRescheduleFuture == null || previousRescheduleFuture.isDone()) {
			try {
				updateState();
				previousPlacementAction = getPreviousPlacementAction();
				potentialPlacementActions.put(previousPlacementAction, getOverallThroughput());
				potentialPlacementActions.put(getTrafficBasedPlacementAction(), 0.0);

				currentPlacementAction = actorCriticModel.selectAction(
					potentialPlacementActions,
					previousPlacementAction,
					getOverallThroughput(),
					new ArrayList<>(getCpuMetrics().values()),
					new ArrayList<>(edgeFlowRates.values()));
				if (currentPlacementAction == null || currentPlacementAction.isEmpty()) {
					currentPlacementAction = getTrafficBasedPlacementAction();
				} else {
					previousPlacementAction = currentPlacementAction;
				}
				log.info("Rescheduling job '" + executionGraph.getJobName() + "'");
				previousRescheduleFuture = rescheduleEager();
			} catch (Exception e) {
				log.error(
					"Encountered exception while trying to reschedule : {}",
					e.getMessage(),
					e);
			}
		}
	}

	private List<Integer> getPreviousPlacementAction() {
		Map<Integer, Integer> currentPlacementTemp = new HashMap<>();
		Map<SchedulingExecutionVertex, Integer> cpuAssignmentMap = getTopLevelContainer()
			.getCurrentCpuAssignment();
		if (cpuAssignmentMap != null && cpuAssignmentMap.size() == nVertices) {
			AtomicInteger vertexCount = new AtomicInteger(1);
			IterableUtils
				.toStream(schedulingTopology.getVertices())
				.forEachOrdered(schedulingExecutionVertex -> {
					currentPlacementTemp.put(
						cpuAssignmentMap.get(schedulingExecutionVertex),
						vertexCount.getAndIncrement());
				});
		} else {
			log.warn("Could not retrieve CPU assignment for this job.");
		}

		return new ArrayList<>(currentPlacementTemp.values());
	}

	private List<Integer> getTrafficBasedPlacementAction() {
		SchedulingExecutionContainer topLevelContainer = getTopLevelContainer();
		topLevelContainer.releaseAllExecutionVertices();
		Set<SchedulingExecutionVertex> unassignedVertices = new HashSet<>();
		Set<SchedulingExecutionVertex> assignedVertices = new HashSet<>();
		Map<Integer, Integer> placementAction = new HashMap<>();
		AtomicInteger placementIndex = new AtomicInteger(1);
		orderedEdgeList.forEach(schedulingExecutionEdge -> {
			SchedulingExecutionVertex sourceVertex = schedulingExecutionEdge.getSourceSchedulingExecutionVertex();
			SchedulingExecutionVertex targetVertex = schedulingExecutionEdge.getTargetSchedulingExecutionVertex();

			boolean sourceVertexAssigned = assignedVertices.contains(sourceVertex);
			boolean targetVertexAssigned = assignedVertices.contains(targetVertex);

			if (!sourceVertexAssigned && !targetVertexAssigned) {
				List<Integer> cpuIds = topLevelContainer.tryScheduleInSameContainer(
					sourceVertex, targetVertex);
				if (cpuIds.size() >= 2) {
					placementAction.put(placementIndex.getAndIncrement(), cpuIds.get(0));
					placementAction.put(placementIndex.getAndIncrement(), cpuIds.get(1));
					assignedVertices.add(sourceVertex);
					assignedVertices.add(targetVertex);
					unassignedVertices.remove(sourceVertex);
					unassignedVertices.remove(targetVertex);
				} else {
					unassignedVertices.add(sourceVertex);
					unassignedVertices.add(targetVertex);
				}
			} else if (!targetVertexAssigned) {
				int cpuId = topLevelContainer.scheduleExecutionVertex(
					targetVertex);
				if (cpuId != -1) {
					placementAction.put(placementIndex.getAndIncrement(), cpuId);
					assignedVertices.add(targetVertex);
					unassignedVertices.remove(targetVertex);
				} else {
					unassignedVertices.add(targetVertex);
				}
			} else if (!sourceVertexAssigned) {
				int cpuId = topLevelContainer.scheduleExecutionVertex(
					sourceVertex);
				if (cpuId != -1) {
					placementAction.put(placementIndex.getAndIncrement(), cpuId);
					assignedVertices.add(sourceVertex);
					unassignedVertices.remove(sourceVertex);
				} else {
					unassignedVertices.add(sourceVertex);
				}
			}
		});
		unassignedVertices.forEach(schedulingExecutionVertex -> {
			int cpuId = topLevelContainer.scheduleExecutionVertex(
				schedulingExecutionVertex);
			if (cpuId == -1) {
				throw new FlinkRuntimeException(
					"Cannot allocate a CPU for executing operator with ID "
						+ schedulingExecutionVertex.getId());
			} else {
				placementAction.put(placementIndex.getAndIncrement(), cpuId);
			}
		});

		return placementAction
			.entrySet()
			.stream()
			.sorted(Map.Entry.comparingByKey())
			.map(Map.Entry::getValue)
			.collect(
				Collectors.toList());
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
