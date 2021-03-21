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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmaster.slotpool.SlotInfoWithUtilization;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionEdge;
import org.apache.flink.runtime.scheduler.adapter.PhysicalExecutionEdge;
import org.apache.flink.runtime.scheduler.adapter.SchedulingCluster;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionContainer;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionEdge;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingRuntimeState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.IterableUtils;

import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.CpuLayout;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public abstract class AbstractSchedulingAgent implements SchedulingAgent, SchedulingRuntimeState {

	protected final SchedulingTopology schedulingTopology;
	protected final Logger log;
	protected final int nCpus;
	protected final int nVertices;
	protected List<Tuple3<TaskManagerLocation, Integer, Integer>> suggestedPlacementAction;
	protected List<Tuple3<TaskManagerLocation, Integer, Integer>> currentPlacementAction;
	protected ScheduledFuture<?> updateExecutor;
	protected final ExecutionGraph executionGraph;
	protected List<SchedulingExecutionEdge> orderedEdgeList;

	private final Map<String, Double> interOpEdgeThroughput;
	private final Map<String, SchedulingExecutionEdge> edgeMap;
	private final List<SchedulingExecutionVertex> sourceVertices = new ArrayList<>();
	private final long triggerPeriod;
	private final SchedulingCluster schedulingCluster;
	private final long waitTimeout;
	private final int numRetries;
	private final CpuLayout cpuLayout;
	private final SchedulingStrategy schedulingStrategy;
	private final SlotPool slotPool;
	private InfluxDBMetricsClient influxDBMetricsClient;
	private double mostRecentArrivalRate = 0d;
	private Map<String, TaskManagerLocation> taskManagerLocationMap;
	private final Map<String, Double> maxPhysicalEdgeThroughput = new HashMap<>();
	private final Map<String, Double> maxLogicalEdgeThroughput = new HashMap<>();
	private final Map<String, Integer> orderedOperatorMap = new HashMap<>();

	public AbstractSchedulingAgent(
		Logger log,
		long triggerPeriod,
		ExecutionGraph executionGraph,
		SchedulingStrategy schedulingStrategy,
		SlotPool slotPool,
		long waitTimeout,
		int numRetries) {

		this.executionGraph = checkNotNull(executionGraph);
		this.schedulingStrategy = checkNotNull(schedulingStrategy);
		this.slotPool = checkNotNull(slotPool);
		this.schedulingTopology = checkNotNull(executionGraph.getSchedulingTopology());
		this.log = log;
		this.cpuLayout = AffinityLock.cpuLayout();
		this.nCpus = cpuLayout.cpus();
		this.interOpEdgeThroughput = new HashMap<>();
		this.edgeMap = new HashMap<>();
		this.triggerPeriod = triggerPeriod;

		//TODO Currently using the same CPU layout across the cluster, which is wrong!
		populateResourceInfo();
		this.schedulingCluster = new SchedulingCluster(
			this.taskManagerLocationMap.values(),
			this.cpuLayout,
			log);

		init();
		this.waitTimeout = waitTimeout;
		this.numRetries = numRetries;
		this.currentPlacementAction = new ArrayList<>();
		this.nVertices = executionGraph.getTotalNumberOfVertices();
		this.schedulingStrategy.setTopLevelContainer(getTopLevelContainer());
	}

	private void populateResourceInfo() {
		slotPool.getAvailableSlotsInformation().forEach(schedulingCluster::addTaskSlot);
		this.taskManagerLocationMap = slotPool
			.getAvailableSlotsInformation()
			.stream()
			.collect(Collectors.toMap(
				info -> info.getTaskManagerLocation().getResourceID().getResourceIdString(),
				SlotInfoWithUtilization::getTaskManagerLocation));
		taskManagerLocationMap.forEach((tmLocResourceId, tmLoc) -> log.info(
			"TaskManager with ID {} available at {} using data port {}",
			tmLocResourceId,
			tmLoc.address().getHostAddress(),
			tmLoc.dataPort()));
		taskManagerLocationMap
			.values()
			.stream()
			.map(TaskManagerLocation::address)
			.map(InetAddress::getHostAddress)
			.forEach(tmAddress -> {
				for (int cpuId = 0; cpuId < nCpus; cpuId++) {
					String cpuIdString = tmAddress + ":" + cpuLayout.socketId(cpuId) + ":" + cpuId;
					schedulingCluster.addCpu(cpuIdString);
				}
			});
	}

	protected void init() {
		AtomicInteger operatorCount = new AtomicInteger(0);
		schedulingTopology.getVertices().forEach(schedulingExecutionVertex -> {
			orderedOperatorMap.put(
				schedulingExecutionVertex.getId().toString(),
				operatorCount.getAndIncrement());
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
		setupInfluxDBConnection();
		updateStateInformation();
	}

	private void logCurrentStatusInformation() {
		if (log.isDebugEnabled()) {
			StringBuilder currentPlacement = new StringBuilder("[");
			schedulingTopology
				.getVertices()
				.forEach(sourceVertex -> currentPlacement
					.append("{Vertex Name: ")
					.append(sourceVertex.getTaskName())
					.append(", CPU ID: ")
					.append(sourceVertex.getExecutionPlacement().getCpuId())
					.append(", CPU Usage: ")
					.append(sourceVertex.getCurrentCpuUsage())
					.append("}, "));
			currentPlacement.append("]");
			log.debug("Current scheduling placement : {}", currentPlacement);
			log.debug(
				"Keeping {} maximum logical edge throughput values of {}",
				maxLogicalEdgeThroughput.size(),
				maxLogicalEdgeThroughput.values());
			log.debug(
				"Keeping {} maximum physical edge throughput values of {} ",
				maxPhysicalEdgeThroughput.size(),
				maxPhysicalEdgeThroughput.values());
		}
	}

	protected Map<String, Double> getCpuUsageMetrics() {
		return influxDBMetricsClient.getCpuUsageMetrics(nCpus);
	}

	protected Map<String, Double> getCpuFrequencyMetrics() {
		return influxDBMetricsClient.getCpuFrequencyMetrics(nCpus);
	}

	protected void updateStateInformation() {
		Map<String, Double> currentInterOpEdgeThroughput = influxDBMetricsClient.getRateMetricsFor(
			"taskmanager_job_task_edge_numRecordsProcessedPerSecond",
			"edge_id",
			"rate");
		Map<String, Double> cpuUsageMetrics = influxDBMetricsClient.getCpuUsageMetrics(nCpus);
		Map<String, Double> cpuFrequencyMetrics = influxDBMetricsClient.getCpuFrequencyMetrics(nCpus);
		Map<String, Double> operatorUsageMetrics = influxDBMetricsClient.getOperatorUsageMetrics();
		schedulingCluster.updateResourceUsageMetrics(
			SchedulingExecutionContainer.CPU,
			cpuUsageMetrics);
		schedulingCluster.updateResourceUsageMetrics(
			SchedulingExecutionContainer.FREQ,
			cpuFrequencyMetrics);
		schedulingCluster.updateResourceUsageMetrics(
			SchedulingExecutionContainer.OPERATOR,
			operatorUsageMetrics);
		Map<String, Double> filteredFlowRates = currentInterOpEdgeThroughput
			.entrySet()
			.stream()
			.filter(mapEntry -> edgeMap.containsKey(mapEntry.getKey()))
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		interOpEdgeThroughput.putAll(filteredFlowRates);
		updateMaxEdgeThroughputMatrices(interOpEdgeThroughput);
		orderedEdgeList = interOpEdgeThroughput
			.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
			.map(mapEntry -> edgeMap.get(mapEntry.getKey())).collect(Collectors.toList());
		mostRecentArrivalRate = influxDBMetricsClient.getMostRecentArrivalRate(sourceVertices
			.stream()
			.map(SchedulingExecutionVertex::getId)
			.map(ExecutionVertexID::toString)
			.collect(Collectors.toList()));
		logCurrentStatusInformation();
	}

	private String getCpuIdAsString(Tuple3<TaskManagerLocation, Integer, Integer> cpuIdTuple) {
		return cpuIdTuple.f0 + SchedulingExecutionContainer.CPU_ID_DELIMITER + cpuIdTuple.f2
			+ SchedulingExecutionContainer.CPU_ID_DELIMITER + cpuIdTuple.f1;
	}

	private void updateMaxEdgeThroughputMatrices(
		Map<String, Double> currentInterOpEdgeThroughput) {

		Map<SchedulingExecutionVertex, Tuple3<TaskManagerLocation, Integer, Integer>> currentCpuAssignment = schedulingCluster
			.getCurrentCpuAssignment();
		currentInterOpEdgeThroughput.forEach((edgeId, edgeRate) -> {
			SchedulingExecutionEdge edge = edgeMap.get(edgeId);
			SchedulingExecutionVertex sourceEV = edge.getSourceSchedulingExecutionVertex();
			SchedulingExecutionVertex targetEV = edge.getTargetSchedulingExecutionVertex();
			PhysicalExecutionEdge peEdge = new PhysicalExecutionEdge(
				sourceEV.getId().getJobVertexId().toString(),
				targetEV.getId().getJobVertexId().toString(),
				getCpuIdAsString(currentCpuAssignment.get(sourceEV)),
				getCpuIdAsString(currentCpuAssignment.get(targetEV)));
			Double currentMaxPhysicalEdgeThroughput = maxPhysicalEdgeThroughput.get(peEdge.getPhysicalExecutionEdgeId());
			if (currentMaxPhysicalEdgeThroughput == null
				|| currentMaxPhysicalEdgeThroughput < edgeRate) {
				maxPhysicalEdgeThroughput.put(peEdge.getPhysicalExecutionEdgeId(), edgeRate);
			}
			String logicalEdgeId = sourceEV.getId().getJobVertexId().toString() + "@" +
				targetEV.getId().getJobVertexId().toString();
			Double currentLogicalEdgeThroughput = maxLogicalEdgeThroughput.get(logicalEdgeId);
			if (currentLogicalEdgeThroughput == null || currentLogicalEdgeThroughput < edgeRate) {
				maxLogicalEdgeThroughput.put(logicalEdgeId, edgeRate);
			}
		});
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
	public Map<String, Double> getInterOpEdgeThroughput() {
		return interOpEdgeThroughput;
	}

	@Override
	public List<SchedulingExecutionEdge> getOrderedEdgeList() {
		return orderedEdgeList;
	}

	@Override
	public SchedulingExecutionContainer getTopLevelContainer() {
		return schedulingCluster;
	}

	@Override
	public List<Tuple3<TaskManagerLocation, Integer, Integer>> getPlacementSolution() {
		return suggestedPlacementAction;
	}

	@Override
	public double getOverallThroughput() {
		return interOpEdgeThroughput.values().stream().mapToDouble(Double::doubleValue).sum();
	}

	@Override
	public double getArrivalRate() {
		return mostRecentArrivalRate;
	}

	@Override
	public void shutdownAgent() {
		log.info(
			"Shutting down scheduling agent for {} scheduling mode",
			executionGraph.getScheduleMode());
		if (updateExecutor != null) {
			updateExecutor.cancel(true);
		}
		influxDBMetricsClient.closeConnection();
	}

	protected abstract void updatePlacementSolution();

	protected void updateCurrentPlacementInformation() {
		Map<SchedulingExecutionVertex, Tuple3<TaskManagerLocation, Integer, Integer>> cpuAssignmentMap = getTopLevelContainer()
			.getCurrentCpuAssignment();

		Map<Integer, Tuple3<TaskManagerLocation, Integer, Integer>> currentPlacementTemp = new HashMap<>();
		if (cpuAssignmentMap != null && cpuAssignmentMap.size() == nVertices) {
			AtomicInteger vertexCount = new AtomicInteger(1);
			IterableUtils
				.toStream(schedulingTopology.getVertices())
				.forEachOrdered(schedulingExecutionVertex -> currentPlacementTemp.put(
					vertexCount.getAndIncrement(),
					cpuAssignmentMap.get(schedulingExecutionVertex)));
		} else {
			log.warn("Could not retrieve current CPU assignment for this job.");
		}

		if (!currentPlacementTemp.isEmpty()) {
			currentPlacementAction = new ArrayList<>(currentPlacementTemp.values());
		}
	}

	@Override
	public boolean isValidPlacementAction(List<Tuple3<TaskManagerLocation, Integer, Integer>> suggestedPlacementAction) {
		List<Integer> suggestedCpuIds = suggestedPlacementAction
			.stream()
			.map(tuple -> (Integer) tuple.getField(1))
			.collect(Collectors.toList());
		return suggestedCpuIds.size() == nVertices
			&& suggestedCpuIds
			.stream()
			.noneMatch(cpuId -> cpuId < 0 || cpuId > (nCpus - 1))
			&& suggestedPlacementAction.stream().distinct().count()
			== suggestedPlacementAction.size();
	}

	protected CompletableFuture<Collection<Acknowledge>> rescheduleEager() {
		checkState(executionGraph.getState() == JobStatus.RUNNING, "job is not running currently");

		final Iterable<ExecutionVertex> vertices = executionGraph.getAllExecutionVertices();
		final ArrayList<CompletableFuture<Acknowledge>> allHaltFutures = new ArrayList<>();

		for (ExecutionVertex ev : vertices) {
			Execution attempt = ev.getCurrentExecutionAttempt();
			CompletableFuture<Acknowledge> haltFuture = attempt
				.haltExecution()
				.whenCompleteAsync((ack, fail) -> {
					String taskNameWithSubtaskIndex = attempt
						.getVertex()
						.getTaskNameWithSubtaskIndex();
					for (int i = 0; i < numRetries; i++) {
						if (attempt.getState() != ExecutionState.CREATED) {
							try {
								Thread.sleep(waitTimeout);
							} catch (InterruptedException exception) {
								log.warn(
									"Thread waiting on halting of task {} was interrupted due to cause : {}",
									taskNameWithSubtaskIndex,
									exception);
							}
						} else {
							if (log.isDebugEnabled()) {
								log.debug("Task '" + taskNameWithSubtaskIndex
									+ "' changed to expected state (" +
									ExecutionState.CREATED + ") while waiting " + i + " times");
							}
							return;
						}
					}
					log.error("Couldn't halt execution for task {}.", taskNameWithSubtaskIndex);
					FutureUtils.completedExceptionally(new Exception(
						"Couldn't halt execution for task " + taskNameWithSubtaskIndex));
				});
			allHaltFutures.add(haltFuture);
		}
		final FutureUtils.ConjunctFuture<Collection<Acknowledge>> allHaltsFuture = FutureUtils.combineAll(
			allHaltFutures);
		return allHaltsFuture.whenComplete((ack, fail) -> {
			if (fail != null) {
				log.error("Encountered exception when halting process.", fail);
				throw new CompletionException("Halt process unsuccessful", fail);
			} else {
				schedulingStrategy.startScheduling(this);
			}
		});
	}
}
