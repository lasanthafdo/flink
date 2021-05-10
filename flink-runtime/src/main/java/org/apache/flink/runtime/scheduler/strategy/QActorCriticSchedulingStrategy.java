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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.execution.ExecutionPlacement;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkRuntimeException;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
			initTaskDeploymentThread.setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE);
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
			schedulingTopology
				.getVertices()
				.forEach(schedulingExecutionVertex -> {
					Tuple3<TaskManagerLocation, Integer, Integer> placementInfo = topLevelContainer.scheduleVertex(
						schedulingExecutionVertex);
					schedulingExecutionVertex.setExecutionPlacement(
						new ExecutionPlacement(
							placementInfo.f0, placementInfo.f1, placementInfo.f2, taskPerCore));
				});
		} else {
			throw new FlinkRuntimeException(
				"Could not obtain top level scheduling container for " + this
					.getClass()
					.getSimpleName());
		}
	}

	private void setupDerivedPlacement(@NotNull SchedulingRuntimeState runtimeState) {
		SchedulingExecutionContainer topLevelContainer = runtimeState.getTopLevelContainer();
		List<Tuple3<TaskManagerLocation, Integer, Integer>> placementAction = runtimeState.getPlacementSolution();
		runtimeState.logPlacementAction(new Tuple2<>(-1, -1), placementAction);
		if (runtimeState.isValidPlacementAction(placementAction)) {
			topLevelContainer.releaseAllExecutionVertices();
			AtomicInteger placementIndex = new AtomicInteger(0);
			schedulingTopology.getVertices().forEach(schedulingExecutionVertex -> {
				Tuple3<TaskManagerLocation, Integer, Integer> placementSuggestion = placementAction.get(
					placementIndex.getAndIncrement());
				Tuple3<TaskManagerLocation, Integer, Integer> placementInfo = topLevelContainer.scheduleVertex(
					schedulingExecutionVertex,
					placementSuggestion.f0,
					placementSuggestion.f2);
				schedulingExecutionVertex.setExecutionPlacement(new ExecutionPlacement(
					placementInfo.f0, placementInfo.f1, placementInfo.f2, taskPerCore));
			});
			log.info(topLevelContainer.getStatus());
		} else {
			throw new FlinkRuntimeException(
				"Suggested operator placement action " + placementAction + " is invalid");
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
