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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.execution.ExecutionPlacement;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.DRLSchedulingAgent;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.util.FlinkRuntimeException;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link SchedulingStrategy} instance for streaming job which will schedule all tasks at the same time.
 */
public class DRLSchedulingStrategy implements SchedulingStrategy {

	private final SchedulerOperations schedulerOperations;

	private final SchedulingTopology schedulingTopology;

	private final DeploymentOption deploymentOption = new DeploymentOption(false);


	public DRLSchedulingStrategy(
		SchedulerOperations schedulerOperations,
		SchedulingTopology schedulingTopology) {

		this.schedulerOperations = checkNotNull(schedulerOperations);
		this.schedulingTopology = checkNotNull(schedulingTopology);
	}

	@Override
	public void startScheduling() {
		allocateSlotsAndDeploy(SchedulingStrategyUtils.getAllVertexIdsFromTopology(
			schedulingTopology), null);
	}

	@Override
	public void startScheduling(SchedulingRuntimeState runtimeState) {
		allocateSlotsAndDeploy(SchedulingStrategyUtils.getAllVertexIdsFromTopology(
			schedulingTopology), runtimeState);
	}

	@Override
	public void restartTasks(Set<ExecutionVertexID> verticesToRestart) {
		allocateSlotsAndDeploy(verticesToRestart, null);
	}

	@Override
	public void onExecutionStateChange(
		ExecutionVertexID executionVertexId,
		ExecutionState executionState) {
		// Will not react to these notifications.
	}

	@Override
	public void onPartitionConsumable(IntermediateResultPartitionID resultPartitionId) {
		// Will not react to these notifications.
	}

	private void allocateSlotsAndDeploy(
		final Set<ExecutionVertexID> verticesToDeploy,
		@Nullable SchedulingRuntimeState runtimeState) {
		if (runtimeState == null) {
			setupDefaultPlacement();
		} else {
			setupDRLPlacement(runtimeState);
		}
		final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions =
			SchedulingStrategyUtils.createExecutionVertexDeploymentOptionsInTopologicalOrder(
				schedulingTopology,
				verticesToDeploy,
				id -> deploymentOption,
				id -> schedulingTopology.getVertex(id).getExecutionPlacement());
		schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
	}

	private void setupDefaultPlacement() {
		AtomicInteger sourceCpuId = new AtomicInteger(2);
		AtomicInteger operatorCpuId = new AtomicInteger(8);
		schedulingTopology.getVertices().forEach(schedulingExecutionVertex -> {
			boolean isSourceVertex = schedulingExecutionVertex
				.getConsumedResults()
				.iterator()
				.hasNext();
			if (isSourceVertex) { // Source vertex
				schedulingExecutionVertex.setExecutionPlacement(new ExecutionPlacement(
					"localhost:0",
					sourceCpuId.getAndAdd(2)));
			} else {
				schedulingExecutionVertex.setExecutionPlacement(new ExecutionPlacement(
					"localhost:0",
					operatorCpuId.getAndIncrement()));
			}
		});
	}

	private void setupDRLPlacement(@NotNull SchedulingRuntimeState runtimeState) {
		SchedulingExecutionContainer topLevelContainer = runtimeState.getTopLevelContainer();
		topLevelContainer.releaseAllExecutionVertices();
		List<SchedulingExecutionEdge> orderedEdgeList = runtimeState.getOrderedEdgeList();
		List<SchedulingExecutionVertex> strandedVertices = new ArrayList<>();
		orderedEdgeList.forEach(schedulingExecutionEdge -> {
			SchedulingExecutionVertex sourceVertex = schedulingExecutionEdge.getSourceSchedulingExecutionVertex();
			SchedulingExecutionVertex targetVertex = schedulingExecutionEdge.getTargetSchedulingExecutionVertex();

			List<Integer> cpuIds = topLevelContainer.tryScheduleInSameContainer(
				sourceVertex,
				targetVertex);
			if (cpuIds != null) {
				if (cpuIds.size() >= 1) {
					sourceVertex.setExecutionPlacement(new ExecutionPlacement(
						"localhost:0",
						cpuIds.get(0)));
					if (cpuIds.size() >= 2) {
						targetVertex.setExecutionPlacement(new ExecutionPlacement(
							"localhost:0",
							cpuIds.get(1)));
					} else {
						strandedVertices.add(targetVertex);
					}
				} else {
					strandedVertices.add(sourceVertex);
					strandedVertices.add(targetVertex);
				}
			}
		});
		strandedVertices.forEach(schedulingExecutionVertex -> {
			int cpuId = topLevelContainer.scheduleExecutionVertex(
				schedulingExecutionVertex);
			if (cpuId == -1) {
				throw new FlinkRuntimeException(
					"Cannot allocate a CPU for executing operator with ID "
						+ schedulingExecutionVertex.getId());
			} else {
				schedulingExecutionVertex.setExecutionPlacement(new ExecutionPlacement(
					"localhost:0",
					cpuId
				));
			}
		});
	}

	/**
	 * The factory for creating {@link DRLSchedulingStrategy}.
	 */
	public static class Factory implements SchedulingStrategyFactory {

		@Override
		public SchedulingStrategy createInstance(
			SchedulerOperations schedulerOperations,
			SchedulingTopology schedulingTopology) {
			return new DRLSchedulingStrategy(schedulerOperations, schedulingTopology);
		}
	}
}
