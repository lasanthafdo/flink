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
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link SchedulingStrategy} instance for streaming job which will schedule all tasks at the same time.
 */
public class AdaptiveSchedulingStrategy implements SchedulingStrategy {

	private final SchedulerOperations schedulerOperations;

	private final SchedulingTopology schedulingTopology;

	private final DeploymentOption deploymentOption = new DeploymentOption(false);

	private SchedulingExecutionContainer topLevelContainer;

	public AdaptiveSchedulingStrategy(
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
			setupAdaptivePlacement(runtimeState);
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
		AtomicInteger currentCpuId = new AtomicInteger(2);
		schedulingTopology.getVertices().forEach(schedulingExecutionVertex -> {
			if (topLevelContainer != null) {
				topLevelContainer.forceSchedule(schedulingExecutionVertex, currentCpuId.get());
			}
			schedulingExecutionVertex.setExecutionPlacement(new ExecutionPlacement(
				DEFAULT_TASK_MANAGER_ADDRESS,
				currentCpuId.getAndIncrement()));
		});
	}

	private void setupAdaptivePlacement(@NotNull SchedulingRuntimeState runtimeState) {
		SchedulingExecutionContainer topLevelContainer = runtimeState.getTopLevelContainer();
		List<Integer> placementSolution = runtimeState.getPlacementSolution();
		topLevelContainer.releaseAllExecutionVertices();
		AtomicInteger placementIndex = new AtomicInteger(0);
		schedulingTopology
			.getVertices()
			.forEach(schedulingExecutionVertex -> {
				topLevelContainer.forceSchedule(
					schedulingExecutionVertex,
					placementSolution.get(placementIndex.get()));
				schedulingExecutionVertex.setExecutionPlacement(new ExecutionPlacement(
					DEFAULT_TASK_MANAGER_ADDRESS,
					placementSolution.get(placementIndex.getAndIncrement())));
			});
	}

	@Override
	public void setTopLevelContainer(SchedulingExecutionContainer topLevelContainer) {
		this.topLevelContainer = topLevelContainer;
	}

	/**
	 * The factory for creating {@link AdaptiveSchedulingStrategy}.
	 */
	public static class Factory implements SchedulingStrategyFactory {

		@Override
		public SchedulingStrategy createInstance(
			SchedulerOperations schedulerOperations,
			SchedulingTopology schedulingTopology) {
			return new AdaptiveSchedulingStrategy(schedulerOperations, schedulingTopology);
		}
	}
}
