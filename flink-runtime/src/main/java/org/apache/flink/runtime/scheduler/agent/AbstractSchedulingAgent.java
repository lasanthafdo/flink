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

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionEdge;
import org.apache.flink.runtime.scheduler.adapter.SchedulingNode;

import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.CpuLayout;

import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionContainer;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionEdge;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingRuntimeState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class AbstractSchedulingAgent implements SchedulingAgent, SchedulingRuntimeState {

	protected final SchedulingTopology schedulingTopology;
	protected final Logger log;
	protected final CpuLayout cpuLayout;
	protected final int nCpus;
	protected final Map<String, Double> edgeFlowRates;
	protected final Map<String, SchedulingExecutionEdge<? extends SchedulingExecutionVertex, ? extends SchedulingResultPartition>> edgeMap;
	protected final List<SchedulingExecutionVertex> sourceVertices = new ArrayList<>();
	protected final ExecutionGraph executionGraph;
	protected final long triggerPeriod;
	protected final SchedulingNode schedulingNode;
	protected final long waitTimeout;
	protected final int numRetries;
	protected List<SchedulingExecutionEdge> orderedEdgeList;
	protected List<Integer> currentPlacementAction;
	protected List<Integer> previousPlacementAction;
	protected ScheduledFuture<?> updateExecutor;
	private InfluxDBMetricsClient influxDBMetricsClient;

	public AbstractSchedulingAgent(
		Logger log,
		long triggerPeriod,
		ExecutionGraph executionGraph,
		long waitTimeout,
		int numRetries) {

		this.executionGraph = checkNotNull(executionGraph);
		this.schedulingTopology = checkNotNull(executionGraph.getSchedulingTopology());
		this.log = log;
		this.cpuLayout = AffinityLock.cpuLayout();
		this.nCpus = cpuLayout.cpus();
		this.edgeFlowRates = new HashMap<>();
		this.edgeMap = new HashMap<>();
		this.triggerPeriod = triggerPeriod;
		this.schedulingNode = new SchedulingNode(this.cpuLayout, log);
		for (int cpuId = 0; cpuId < nCpus; cpuId++) {
			schedulingNode.addCpu(cpuId);
		}

		initializeEdgeFlowRates();
		setupInfluxDBConnection();
		updateStateInformation();
		this.waitTimeout = waitTimeout;
		this.numRetries = numRetries;
		this.previousPlacementAction = new ArrayList<>();
	}

	protected void initializeEdgeFlowRates() {
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
						log.info("Execution ID: " + dee.getExecutionEdgeId());
						edgeMap.put(dee.getExecutionEdgeId(), dee);
					});
				consumerCount.getAndIncrement();
			});
			if (consumerCount.get() == 0) {
				sourceVertices.add(schedulingExecutionVertex);
			}
		});
	}

	private void logCurrentPlacement() {
		StringBuilder currentPlacement = new StringBuilder("[");
		schedulingTopology.getVertices().forEach(sourceVertex -> {
			currentPlacement.append("{Vertex Name: ").append(sourceVertex.getTaskName())
				.append(", CPU ID: ").append(sourceVertex.getExecutionPlacement().getCpuId())
				.append(", CPU Usage: ").append(sourceVertex.getCurrentCpuUsage())
				.append("}, ");

		});
		currentPlacement.append("]");
		log.info("Current scheduling placement is : {}", currentPlacement);
	}

	protected Map<String, Double> getCpuMetrics() {
		return influxDBMetricsClient.getCpuMetrics(nCpus);
	}

	protected void updateStateInformation() {
		Map<String, Double> currentFlowRates = influxDBMetricsClient.getRateMetricsFor(
			"taskmanager_job_task_edge_numRecordsProcessedPerSecond",
			"edge_id",
			"rate");
		Map<String, Double> cpuMetrics = influxDBMetricsClient.getCpuMetrics(nCpus);
		Map<String, Double> operatorUsageMetrics = influxDBMetricsClient.getOperatorUsageMetrics();
		schedulingNode.updateResourceUsageMetrics(
			SchedulingExecutionContainer.CPU,
			cpuMetrics);
		schedulingNode.updateResourceUsageMetrics(
			SchedulingExecutionContainer.OPERATOR,
			operatorUsageMetrics);
		Map<String, Double> filteredFlowRates = currentFlowRates
			.entrySet()
			.stream()
			.filter(mapEntry -> edgeMap.containsKey(mapEntry.getKey()))
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		edgeFlowRates.putAll(filteredFlowRates);
		orderedEdgeList = edgeFlowRates
			.entrySet()
			.stream()
			.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
			.map(mapEntry -> edgeMap.get(mapEntry.getKey()))
			.collect(Collectors.toList());
		logCurrentPlacement();
	}

	protected void setupInfluxDBConnection() {
		influxDBMetricsClient = new InfluxDBMetricsClient("http://127.0.0.1:8086", "flink", log);
		influxDBMetricsClient.setup();
	}

	@Override
	public long getTriggerPeriod() {
		return triggerPeriod;
	}

	@Override
	public List<SchedulingExecutionVertex> getSourceVertices() {
		return sourceVertices;
	}

	@Override
	public Map<String, Double> getEdgeThroughput() {
		return edgeFlowRates;
	}

	@Override
	public List<SchedulingExecutionEdge> getOrderedEdgeList() {
		return orderedEdgeList;
	}

	@Override
	public SchedulingExecutionContainer getTopLevelContainer() {
		return schedulingNode;
	}

	@Override
	public List<Integer> getPlacementSolution() {
		return currentPlacementAction;
	}

	@Override
	public double getOverallThroughput() {
		return edgeFlowRates.values().stream().mapToDouble(Double::doubleValue).sum();
	}

	@Override
	public void shutdownAgent() {
		if(updateExecutor != null) {
			updateExecutor.cancel(true);
		}
	}

	protected abstract void setCurrentPlacementSolution();

	protected List<Integer> getTrafficBasedPlacementAction() {
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
}
