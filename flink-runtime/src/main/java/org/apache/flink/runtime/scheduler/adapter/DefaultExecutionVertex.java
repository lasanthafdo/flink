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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.runtime.execution.ExecutionPlacement;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link SchedulingExecutionVertex}.
 */
class DefaultExecutionVertex implements SchedulingExecutionVertex {

	private final ExecutionVertexID executionVertexId;

	private final int subTaskIndex;

	private final String subTaskName;

	private final List<DefaultResultPartition> consumedResults;

	private final List<DefaultResultPartition> producedResults;

	private final Supplier<ExecutionState> stateSupplier;

	private final Supplier<ExecutionPlacement> placementSupplier;

	private final Consumer<ExecutionPlacement> placementConsumer;

	private final InputDependencyConstraint inputDependencyConstraint;

	private double currentCpuUsage;

	DefaultExecutionVertex(
		ExecutionVertexID executionVertexId,
		List<DefaultResultPartition> producedPartitions,
		Supplier<ExecutionState> stateSupplier,
		InputDependencyConstraint constraint) {

		this(executionVertexId, "UNKNOWN", 0, producedPartitions,
			stateSupplier, () -> null, executionPlacement -> {
			}, constraint);
	}

	DefaultExecutionVertex(
		ExecutionVertexID executionVertexId,
		String subTaskName,
		int subTaskIndex,
		List<DefaultResultPartition> producedPartitions,
		Supplier<ExecutionState> stateSupplier,
		Supplier<ExecutionPlacement> placementSupplier,
		Consumer<ExecutionPlacement> placementConsumer,
		InputDependencyConstraint constraint) {

		this.executionVertexId = checkNotNull(executionVertexId);
		this.subTaskName = checkNotNull(subTaskName);
		this.subTaskIndex = subTaskIndex;
		this.consumedResults = new ArrayList<>();
		this.stateSupplier = checkNotNull(stateSupplier);
		this.placementSupplier = checkNotNull(placementSupplier);
		this.placementConsumer = checkNotNull(placementConsumer);
		this.producedResults = checkNotNull(producedPartitions);
		this.inputDependencyConstraint = checkNotNull(constraint);
		this.currentCpuUsage = 0.0;
	}

	@Override
	public ExecutionVertexID getId() {
		return executionVertexId;
	}

	@Override
	public String getTaskName() {
		return subTaskName;
	}

	@Override
	public int getSubTaskIndex() {
		return subTaskIndex;
	}

	@Override
	public ExecutionState getState() {
		return stateSupplier.get();
	}

	@Override
	public ExecutionPlacement getExecutionPlacement() {
		return placementSupplier.get();
	}

	@Override
	public void setExecutionPlacement(ExecutionPlacement executionPlacement) {
		placementConsumer.accept(executionPlacement);
	}

	@Override
	public Iterable<DefaultResultPartition> getConsumedResults() {
		return consumedResults;
	}

	@Override
	public Iterable<DefaultResultPartition> getProducedResults() {
		return producedResults;
	}

	@Override
	public InputDependencyConstraint getInputDependencyConstraint() {
		return inputDependencyConstraint;
	}

	@Override
	public double getCurrentCpuUsage() {
		return currentCpuUsage;
	}

	@Override
	public void setCurrentCpuUsage(double cpuUsage) {
		currentCpuUsage = cpuUsage;
	}

	void addConsumedResult(DefaultResultPartition result) {
		consumedResults.add(result);
	}
}
