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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;

import org.slf4j.Logger;

import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link SchedulingStrategy} instance for streaming job which will schedule all tasks at the same time.
 */
public class PinnedSchedulingStrategy implements SchedulingStrategy {

	private final SchedulerOperations schedulerOperations;

	private final SchedulingTopology schedulingTopology;

	private final DeploymentOption deploymentOption = new DeploymentOption(false);
	private boolean taskPerCore = false;

	public PinnedSchedulingStrategy(
		SchedulerOperations schedulerOperations,
		SchedulingTopology schedulingTopology) {
		this.schedulerOperations = checkNotNull(schedulerOperations);
		this.schedulingTopology = checkNotNull(schedulingTopology);
	}

	@Override
	public void startScheduling() {
		allocateSlotsAndDeploy(SchedulingStrategyUtils.getAllVertexIdsFromTopology(schedulingTopology));
	}

	@Override
	public void startScheduling(SchedulingRuntimeState ignored) {
		startScheduling();
	}

	@Override
	public void setTopLevelContainer(SchedulingExecutionContainer schedulingExecutionContainer) {

	}

	@Override
	public void setTaskPerCoreScheduling(boolean taskPerCoreScheduling) {
		this.taskPerCore = taskPerCoreScheduling;
	}

	@Override
	public void restartTasks(Set<ExecutionVertexID> verticesToRestart) {
		allocateSlotsAndDeploy(verticesToRestart);
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

	private void allocateSlotsAndDeploy(final Set<ExecutionVertexID> verticesToDeploy) {
		final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions =
			SchedulingStrategyUtils.createExecutionVertexDeploymentOptionsInTopologicalOrder(
				schedulingTopology,
				verticesToDeploy,
				id -> deploymentOption);
		schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
	}

	/**
	 * The factory for creating {@link PinnedSchedulingStrategy}.
	 */
	public static class Factory implements SchedulingStrategyFactory {

		@Override
		public SchedulingStrategy createInstance(
			SchedulerOperations schedulerOperations,
			SchedulingTopology schedulingTopology, Logger log) {
			return new PinnedSchedulingStrategy(schedulerOperations, schedulingTopology);
		}
	}
}
