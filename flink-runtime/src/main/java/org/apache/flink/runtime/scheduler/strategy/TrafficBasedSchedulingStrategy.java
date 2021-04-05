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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.execution.ExecutionPlacement;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link SchedulingStrategy} instance for streaming job which will schedule all tasks at the same time.
 */
public class TrafficBasedSchedulingStrategy implements SchedulingStrategy {

	private final SchedulerOperations schedulerOperations;
	private final SchedulingTopology schedulingTopology;
	private final DeploymentOption deploymentOption = new DeploymentOption(false);

	public TrafficBasedSchedulingStrategy(
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
	public void setTopLevelContainer(SchedulingExecutionContainer schedulingExecutionContainer) {

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
		SchedulingRuntimeState runtimeState) {
		final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions;
		if (runtimeState == null || runtimeState.getPlacementSolution().isEmpty()) {
			executionVertexDeploymentOptions =
				SchedulingStrategyUtils.createExecutionVertexDeploymentOptionsInTopologicalOrder(
					schedulingTopology,
					verticesToDeploy,
					id -> deploymentOption);
		} else {
			setupDerivedPlacement(runtimeState);
			executionVertexDeploymentOptions =
				SchedulingStrategyUtils.createExecutionVertexDeploymentOptionsInTopologicalOrder(
					schedulingTopology,
					verticesToDeploy,
					id -> deploymentOption,
					id -> schedulingTopology.getVertex(id).getExecutionPlacement());
		}
		schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
	}

	private void setupDerivedPlacement(SchedulingRuntimeState runtimeState) {
		SchedulingExecutionContainer topLevelContainer = runtimeState.getTopLevelContainer();
		List<Tuple3<TaskManagerLocation, Integer, Integer>> placementAction = runtimeState.getPlacementSolution();
		if (runtimeState.isValidPlacementAction(placementAction)) {
			topLevelContainer.releaseAllExecutionVertices();
			AtomicInteger placementIndex = new AtomicInteger(0);
			schedulingTopology.getVertices().forEach(schedulingExecutionVertex -> {
				topLevelContainer.forceSchedule(
					schedulingExecutionVertex,
					placementAction.get(placementIndex.get()));
				Tuple3<TaskManagerLocation, Integer, Integer> placementInfoTuple = placementAction.get(
					placementIndex.getAndIncrement());
				schedulingExecutionVertex.setExecutionPlacement(new ExecutionPlacement(
					placementInfoTuple.f0, placementInfoTuple.f1, placementInfoTuple.f2));
			});
		} else {
			throw new FlinkRuntimeException(
				"Suggested operator placement action " + placementAction + " is invalid");
		}
	}

	/**
	 * The factory for creating {@link TrafficBasedSchedulingStrategy}.
	 */
	public static class Factory implements SchedulingStrategyFactory {

		@Override
		public SchedulingStrategy createInstance(
			SchedulerOperations schedulerOperations,
			SchedulingTopology schedulingTopology) {
			return new TrafficBasedSchedulingStrategy(schedulerOperations, schedulingTopology);
		}
	}
}
