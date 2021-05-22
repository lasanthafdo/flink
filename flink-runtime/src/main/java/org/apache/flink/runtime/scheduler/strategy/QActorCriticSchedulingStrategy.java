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

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.execution.ExecutionPlacement;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link SchedulingStrategy} instance for streaming job which will schedule all tasks at the same time.
 */
public class QActorCriticSchedulingStrategy implements SchedulingStrategy {

	private final SchedulerOperations schedulerOperations;
	private final SchedulingTopology schedulingTopology;
	private final DeploymentOption deploymentOption = new DeploymentOption(false);
	private boolean taskPerCore = false;
	private SchedulingExecutionContainer topLevelContainer = null;
	private final Logger log;

	public QActorCriticSchedulingStrategy(
		SchedulerOperations schedulerOperations,
		SchedulingTopology schedulingTopology, Logger log) {

		this.schedulerOperations = checkNotNull(schedulerOperations);
		this.schedulingTopology = checkNotNull(schedulingTopology);
		this.log = checkNotNull(log);
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
		this.topLevelContainer = schedulingExecutionContainer;
	}

	@Override
	public void setTaskPerCoreScheduling(boolean taskPerCoreScheduling) {
		this.taskPerCore = taskPerCoreScheduling;
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
		if (topLevelContainer == null) {
			Runnable initialTaskDeployer = new InitialTaskDeployer();
			Thread initTaskDeploymentThread = new Thread(
				Thread.currentThread().getThreadGroup(),
				initialTaskDeployer,
				String.format(
					"Initial task deployer for %s.",
					this.getClass().getSimpleName()));
			initTaskDeploymentThread.setDaemon(true);
			initTaskDeploymentThread.setUncaughtExceptionHandler((t, e) -> log.error(
				"Uncaught exception {} in thread {} of {}",
				e.getMessage(),
				t.getName(),
				QActorCriticSchedulingStrategy.class.getName(),
				e));
			initTaskDeploymentThread.start();
		} else {
			if (runtimeState == null || runtimeState.getPlacementSolution().isEmpty()) {
				setupDefaultPlacement();
			} else {
				setupDerivedPlacement(runtimeState);
			}
			final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions =
				SchedulingStrategyUtils.createExecutionVertexDeploymentOptionsInTopologicalOrder(
					schedulingTopology,
					verticesToDeploy,
					id -> deploymentOption,
					id -> schedulingTopology.getVertex(id).getExecutionPlacement());
			schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
		}
	}

	private void setupDefaultPlacement() {
		if (topLevelContainer != null) {
			List<Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer>> resolvedPlacementAction = new ArrayList<>();
			schedulingTopology
				.getVertices()
				.forEach(schedulingExecutionVertex -> {
					Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer> placementInfo = topLevelContainer
						.scheduleVertex(
							schedulingExecutionVertex);
					schedulingExecutionVertex.setExecutionPlacement(
						new ExecutionPlacement(
							placementInfo.f0, placementInfo.f1, placementInfo.f2,
							placementInfo.f3, taskPerCore));
					resolvedPlacementAction.add(placementInfo);
				});
			logPlacementAction(resolvedPlacementAction);
		} else {
			throw new FlinkRuntimeException(
				"Could not obtain top level scheduling container for " + this
					.getClass()
					.getSimpleName());
		}
	}

	private void setupDerivedPlacement(@NotNull SchedulingRuntimeState runtimeState) {
		SchedulingExecutionContainer topLevelContainer = runtimeState.getTopLevelContainer();
		List<Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer>> placementAction = runtimeState
			.getPlacementSolution();
		List<Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer>> resolvedPlacementAction = new ArrayList<>();
		if (runtimeState.isValidPlacementAction(placementAction)) {
			topLevelContainer.releaseAllExecutionVertices();
			AtomicInteger placementIndex = new AtomicInteger(0);
			schedulingTopology.getVertices().forEach(schedulingExecutionVertex -> {
				Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer> placementSuggestion = placementAction
					.get(
						placementIndex.getAndIncrement());
				Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer> placementInfo = topLevelContainer
					.scheduleVertex(
						schedulingExecutionVertex,
						placementSuggestion.f0,
						placementSuggestion.f3);
				resolvedPlacementAction.add(placementInfo);
				schedulingExecutionVertex.setExecutionPlacement(new ExecutionPlacement(
					placementInfo.f0,
					placementInfo.f1,
					placementInfo.f2,
					placementInfo.f3,
					taskPerCore));
			});
			logPlacementAction(resolvedPlacementAction);
			log.info(topLevelContainer.getStatus());
		} else {
			throw new FlinkRuntimeException(
				"Suggested operator placement action " + placementAction + " is invalid");
		}
	}

	private void logPlacementAction(
		List<Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer>> placementAction) {
		if (placementAction != null && !placementAction.isEmpty()) {
			StringBuilder logMessage = new StringBuilder();
			List<SchedulingExecutionVertex> executionVertices = StreamSupport
				.stream(schedulingTopology.getVertices().spliterator(), false)
				.collect(
					Collectors.toList());
			for (int i = 0; i < placementAction.size(); i++) {
				Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer> currentOpPlacement =
					placementAction.get(i);
				logMessage.append("\n[").append(executionVertices.get(i).getTaskName())
					.append(" / ").append(executionVertices.get(i).getSubTaskIndex())
					.append("(").append(executionVertices.get(i).getId()).append(")")
					.append(" -> ")
					.append(currentOpPlacement.f0 != null ?
						currentOpPlacement.f0.address().getHostAddress() : "null")
					.append(":").append(currentOpPlacement.f1)
					.append(":").append(currentOpPlacement.f3)
					.append(":").append(currentOpPlacement.f2).append("], ");
			}
			log.info("Placement action : {} ", logMessage);
		}
	}

	private class InitialTaskDeployer implements Runnable {

		@Override
		public void run() {
			try {
				int retryCount = 0;
				while (topLevelContainer == null && retryCount < 20) {
					retryCount++;
					try {
						Thread.sleep(500);
					} catch (InterruptedException ignore) {
						//ignore
					}
				}
				allocateSlotsAndDeploy(SchedulingStrategyUtils.getAllVertexIdsFromTopology(
					schedulingTopology), null);
			} catch (Throwable t) {
				throw new FlinkRuntimeException(
					"Unexpected error in Initial Task Deployer thread",
					t);
			}
		}
	}

	/**
	 * The factory for creating {@link QActorCriticSchedulingStrategy}.
	 */
	public static class Factory implements SchedulingStrategyFactory {

		@Override
		public SchedulingStrategy createInstance(
			SchedulerOperations schedulerOperations,
			SchedulingTopology schedulingTopology, Logger log) {
			return new QActorCriticSchedulingStrategy(schedulerOperations, schedulingTopology, log);
		}
	}
}
