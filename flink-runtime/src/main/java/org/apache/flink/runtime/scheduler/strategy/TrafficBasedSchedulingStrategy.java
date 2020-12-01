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

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.execution.ExecutionPlacement;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionEdge;
import org.apache.flink.runtime.scheduler.adapter.SchedulingCpuCore;
import org.apache.flink.runtime.scheduler.adapter.SchedulingCpuSocket;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link SchedulingStrategy} instance for streaming job which will schedule all tasks at the same time.
 */
public class TrafficBasedSchedulingStrategy implements SchedulingStrategy {

	private static final SchedulingCpuSocket schedulingCpuSocket = new SchedulingCpuSocket(new ArrayList<>(
		Arrays.asList(
			new SchedulingCpuCore(new ArrayList<>(Arrays.asList(0, 1))),
			new SchedulingCpuCore(new ArrayList<>(Arrays.asList(2, 3))),
			new SchedulingCpuCore(new ArrayList<>(Arrays.asList(4, 5))),
			new SchedulingCpuCore(new ArrayList<>(Arrays.asList(6, 7))),
			new SchedulingCpuCore(new ArrayList<>(Arrays.asList(8, 9))),
			new SchedulingCpuCore(new ArrayList<>(Arrays.asList(10, 11)))
		)));

	private final SchedulerOperations schedulerOperations;

	private final SchedulingTopology schedulingTopology;

	private InfluxDBMetricsClient influxDBMetricsClient;

	private final DeploymentOption deploymentOption = new DeploymentOption(false);

	private final Map<String, Double> edgeFlowRates;

	private final Map<String, SchedulingExecutionEdge<? extends SchedulingExecutionVertex, ? extends SchedulingResultPartition>> edgeMap;

	private final List<SchedulingExecutionVertex> sourceVertices = new ArrayList<>();

	private List<SchedulingExecutionEdge> orderedEdgeList;

	public TrafficBasedSchedulingStrategy(
		SchedulerOperations schedulerOperations,
		SchedulingTopology schedulingTopology) {

		this.schedulerOperations = checkNotNull(schedulerOperations);
		this.schedulingTopology = checkNotNull(schedulingTopology);
		this.edgeFlowRates = new HashMap<>();
		this.edgeMap = new HashMap<>();
		initializeEdgeFlowRates(schedulingTopology);
		setupInfluxDBConnection();
	}

	private void initializeEdgeFlowRates(SchedulingTopology schedulingTopology) {
		schedulingTopology.getVertices().forEach(schedulingExecutionVertex -> {
			AtomicInteger consumerCount = new AtomicInteger(0);
			schedulingExecutionVertex.getConsumedResults().forEach(schedulingResultPartition -> {
				schedulingResultPartition
					.getConsumers()
					.forEach(consumer -> {
						DefaultExecutionEdge dee = new DefaultExecutionEdge(
							schedulingResultPartition.getProducer(),
							consumer,
							schedulingResultPartition);
						edgeMap.put(dee.getExecutionEdgeId(), dee);
					});
				consumerCount.getAndIncrement();
			});
			if (consumerCount.get() == 0) {
				sourceVertices.add(schedulingExecutionVertex);
			}
		});
	}

	private void setupInfluxDBConnection() {
		influxDBMetricsClient = new InfluxDBMetricsClient("http://127.0.0.1:8086", "flink");
		influxDBMetricsClient.setup();
	}

	@Override
	public void startScheduling() {
		Map<String, Double> currentFlowRates = influxDBMetricsClient.getRateMetricsFor(
			"taskmanager_job_task_edge_numRecordsProcessedPerSecond",
			"edge_id",
			"rate");
		edgeFlowRates.putAll(currentFlowRates);
		orderedEdgeList = edgeFlowRates
			.entrySet()
			.stream()
			.sorted(Map.Entry.comparingByValue())
			.map(mapEntry -> edgeMap.get(mapEntry.getKey()))
			.collect(Collectors.toList());
		allocateSlotsAndDeploy(SchedulingStrategyUtils.getAllVertexIdsFromTopology(
			schedulingTopology));
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
		AtomicInteger sourceCpuId = new AtomicInteger(2);
		AtomicInteger operatorCpuId = new AtomicInteger(8);
		schedulingTopology.getVertices().forEach(schedulingExecutionVertex -> {
			if (sourceVertices.contains(schedulingExecutionVertex)) { // Source vertex
				schedulingExecutionVertex.setExecutionPlacement(new ExecutionPlacement(
					"localhost:0",
					sourceCpuId.getAndAdd(2)));
			} else {
				schedulingExecutionVertex.setExecutionPlacement(new ExecutionPlacement(
					"localhost:0",
					operatorCpuId.getAndIncrement()));
			}
		});
		List<SchedulingExecutionVertex> strandedVertices = new ArrayList<>();
		orderedEdgeList.forEach(schedulingExecutionEdge -> {
			SchedulingExecutionVertex sourceVertex = schedulingExecutionEdge.getSourceSchedulingExecutionVertex();
			SchedulingExecutionVertex targetVertex = schedulingExecutionEdge.getTargetSchedulingExecutionVertex();

			List<Integer> cpuIds = schedulingCpuSocket.tryScheduleInSameContainer(
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
			int cpuId = schedulingCpuSocket.scheduleExecutionVertex(schedulingExecutionVertex);
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
		final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions =
			SchedulingStrategyUtils.createExecutionVertexDeploymentOptionsInTopologicalOrder(
				schedulingTopology,
				verticesToDeploy,
				id -> deploymentOption,
				id -> schedulingTopology.getVertex(id).getExecutionPlacement());
		schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
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
