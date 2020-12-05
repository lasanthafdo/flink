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

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.InfluxDBMetricsClient;
import org.apache.flink.runtime.scheduler.SchedulingAgent;
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionEdge;
import org.apache.flink.runtime.scheduler.adapter.SchedulingCpuSocket;
import org.apache.flink.runtime.scheduler.adapter.SchedulingNode;

import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.CpuLayout;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class AbstractSchedulingAgent implements SchedulingAgent, SchedulingRuntimeState {

	protected final SchedulingTopology schedulingTopology;
	protected final Logger log;
	protected final CpuLayout cpuLayout;
	protected final Map<Integer, SchedulingCpuSocket> cpuSocketMap;
	protected final int nCpus;
	protected final Map<String, Double> edgeFlowRates;
	protected final Map<String, SchedulingExecutionEdge<? extends SchedulingExecutionVertex, ? extends SchedulingResultPartition>> edgeMap;
	protected final List<SchedulingExecutionVertex> sourceVertices = new ArrayList<>();
	protected final ExecutionGraph executionGraph;
	protected final long triggerPeriod;
	protected final SchedulingNode schedulingNode;
	protected List<SchedulingExecutionEdge> orderedEdgeList;
	private InfluxDBMetricsClient influxDBMetricsClient;

	public AbstractSchedulingAgent(
		Logger log, long triggerPeriod, ExecutionGraph executionGraph) {

		this.executionGraph = checkNotNull(executionGraph);
		this.schedulingTopology = checkNotNull(executionGraph.getSchedulingTopology());
		this.log = log;
		this.cpuLayout = AffinityLock.cpuLayout();
		this.cpuSocketMap = new HashMap<>();
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

	protected void updateState() {
		Map<String, Double> currentFlowRates = influxDBMetricsClient.getRateMetricsFor(
			"taskmanager_job_task_edge_numRecordsProcessedPerSecond",
			"edge_id",
			"rate");
		Map<Integer, Double> cpuMetrics = influxDBMetricsClient.getCpuMetrics(12);
		cpuSocketMap.values().forEach(cpuSocket -> {
			cpuSocket.updateResourceUsageMetrics(
				SchedulingExecutionContainer.CPU,
				cpuMetrics);
		});
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
		log.info("CPU usage:\n {}", cpuMetrics);
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
	public double getOverallThroughput() {
		return edgeFlowRates.values().stream().mapToDouble(Double::doubleValue).sum();
	}
}
