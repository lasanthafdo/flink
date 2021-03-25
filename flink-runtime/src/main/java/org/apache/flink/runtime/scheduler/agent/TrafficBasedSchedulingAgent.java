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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionContainer;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A scheduling agent that will run periodically to reschedule.
 */
public class TrafficBasedSchedulingAgent extends AbstractSchedulingAgent {

	private final ScheduledExecutorService executorService;
	private final long updatePeriodInSeconds;

	public TrafficBasedSchedulingAgent(
		Logger log,
		ExecutionGraph executionGraph,
		SchedulingStrategy schedulingStrategy,
		SlotPool slotPool,
		ScheduledExecutorService executorService,
		long triggerPeriod,
		long waitTimeout,
		int numRetries,
		int updatePeriod,
		int scalingFactor) {
		super(
			log,
			triggerPeriod,
			executionGraph,
			schedulingStrategy,
			slotPool,
			waitTimeout,
			numRetries, scalingFactor);

		this.executorService = checkNotNull(executorService);
		this.updatePeriodInSeconds = updatePeriod;
		setupUpdateTriggerThread();
	}

	private void setupUpdateTriggerThread() {
		updateExecutor = executorService.scheduleAtFixedRate(
			this::executeUpdateProcess,
			updatePeriodInSeconds,
			updatePeriodInSeconds,
			TimeUnit.SECONDS);
	}

	private void executeUpdateProcess() {
		try {
			updateStateInformation();
			updateCurrentPlacementInformation();
			updatePlacementSolution();
		} catch (Exception e) {
			log.error(
				"Encountered exception when trying to update state: {}",
				e.getMessage(),
				e);
		}
	}

	@Override
	public void run() {
		if (previousRescheduleFuture == null || previousRescheduleFuture.isDone()) {
			executeUpdateProcess();
			try {
				if (currentPlacementAction.equals(suggestedPlacementAction)) {
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

	@Override
	protected void updatePlacementSolution() {
		suggestedPlacementAction = getTrafficBasedPlacementAction();
		if (!isValidPlacementAction(suggestedPlacementAction)) {
			throw new FlinkRuntimeException(
				"Invalid placement action " + suggestedPlacementAction + " suggested.");
		}
	}

	private List<Tuple3<TaskManagerLocation, Integer, Integer>> getTrafficBasedPlacementAction() {
		SchedulingExecutionContainer topLevelContainer = getTopLevelContainer();
		topLevelContainer.releaseAllExecutionVertices();
		Set<SchedulingExecutionVertex> unassignedVertices = new HashSet<>();
		Set<SchedulingExecutionVertex> assignedVertices = new HashSet<>();
		Map<Integer, Tuple3<TaskManagerLocation, Integer, Integer>> placementAction = new HashMap<>();
		AtomicInteger placementIndex = new AtomicInteger(1);

		orderedEdgeList.forEach(schedulingExecutionEdge -> {
			SchedulingExecutionVertex sourceVertex = schedulingExecutionEdge.getSourceSchedulingExecutionVertex();
			SchedulingExecutionVertex targetVertex = schedulingExecutionEdge.getTargetSchedulingExecutionVertex();

			boolean sourceVertexAssigned = assignedVertices.contains(sourceVertex);
			boolean targetVertexAssigned = assignedVertices.contains(targetVertex);

			if (!sourceVertexAssigned && !targetVertexAssigned) {
				List<Tuple3<TaskManagerLocation, Integer, Integer>> cpuIds = topLevelContainer.tryScheduleInSameContainer(
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
				Tuple3<TaskManagerLocation, Integer, Integer> cpuId = topLevelContainer.scheduleExecutionVertex(
					targetVertex);
				if (cpuId.getField(0) != null) {
					placementAction.put(placementIndex.getAndIncrement(), cpuId);
					assignedVertices.add(targetVertex);
					unassignedVertices.remove(targetVertex);
				} else {
					unassignedVertices.add(targetVertex);
				}
			} else if (!sourceVertexAssigned) {
				Tuple3<TaskManagerLocation, Integer, Integer> cpuId = topLevelContainer.scheduleExecutionVertex(
					sourceVertex);
				if (cpuId.getField(0) != null) {
					placementAction.put(placementIndex.getAndIncrement(), cpuId);
					assignedVertices.add(sourceVertex);
					unassignedVertices.remove(sourceVertex);
				} else {
					unassignedVertices.add(sourceVertex);
				}
			}
		});
		unassignedVertices.forEach(schedulingExecutionVertex -> {
			Tuple3<TaskManagerLocation, Integer, Integer> cpuId = topLevelContainer.scheduleExecutionVertex(
				schedulingExecutionVertex);
			if (cpuId.getField(0) == null) {
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
}
