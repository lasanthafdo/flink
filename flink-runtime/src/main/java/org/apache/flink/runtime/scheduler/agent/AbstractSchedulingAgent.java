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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionPlacement;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
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
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.util.IterableUtils;

import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.CpuLayout;
import org.slf4j.Logger;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.scheduler.agent.SchedulingAgentUtils.getVertexName;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public abstract class AbstractSchedulingAgent implements SchedulingAgent, SchedulingRuntimeState {

	public static final String NULL_CPU_ID = "127.0.0.1:-1:-1";
	public static final double INTER_NODE_TRAFFIC_SCALING_FACTOR = 2.0;
	public static final double INTER_SOCKET_TRAFFIC_SCALING_FACTOR = 1.25;
	protected final SchedulingTopology schedulingTopology;
	protected final Logger log;
	protected final int nProcessingUnits;
	protected final int nVertices;
	protected int nSchedulingSlots;
	protected List<Tuple3<TaskManagerLocation, Integer, Integer>> suggestedPlacementAction;
	protected List<Tuple3<TaskManagerLocation, Integer, Integer>> currentPlacementAction;
	protected ScheduledFuture<?> updateExecutor;
	protected final ExecutionGraph executionGraph;
	protected final Map<Tuple3<ExecutionVertexID, String, Integer>, Integer> orderedOperatorMap = new HashMap<>();
	protected List<SchedulingExecutionEdge> orderedEdgeList;
	protected CompletableFuture<Collection<Acknowledge>> previousRescheduleFuture;
	protected final Map<String, Tuple2<TaskManagerLocation, Integer>> taskManLocSlotCountMap = new HashMap<>();
	protected final CpuLayout cpuLayout;
	protected final boolean taskPerCore;
	protected final int maxParallelism;

	private final Map<String, Double> interOpEdgeThroughput;
	private final Map<String, SchedulingExecutionEdge> edgeMap;
	private final Map<String, Double> maxPhysicalEdgeThroughput = new HashMap<>();
	private final Map<String, Double> maxLogicalEdgeThroughput = new HashMap<>();
	private final List<SchedulingExecutionVertex> sourceVertices = new ArrayList<>();
	private final long triggerPeriod;
	private final long waitTimeout;
	private final int numRetries;
	private final SchedulingStrategy schedulingStrategy;
	private SchedulingCluster schedulingCluster;
	private ResourceManagerGateway resourceManagerGateway;
	private InfluxDBMetricsClient influxDBMetricsClient;
	private double mostRecentArrivalRate = 0d;
	private boolean waitingForResourceManager = true;

	public AbstractSchedulingAgent(
		Logger log,
		long triggerPeriod,
		ExecutionGraph executionGraph,
		SchedulingStrategy schedulingStrategy,
		long waitTimeout,
		int numRetries, int maxParallelism, boolean taskPerCore) {

		this.executionGraph = checkNotNull(executionGraph);
		this.schedulingStrategy = checkNotNull(schedulingStrategy);
		this.schedulingTopology = checkNotNull(executionGraph.getSchedulingTopology());
		this.taskPerCore = taskPerCore;
		this.log = log;
		this.cpuLayout = AffinityLock.cpuLayout();
		//TODO Make nCpus user-configurable
		if (this.taskPerCore) {
			this.nProcessingUnits = cpuLayout.coresPerSocket() * cpuLayout.sockets();
		} else {
			this.nProcessingUnits = cpuLayout.cpus();
		}
		this.nSchedulingSlots = 0;
		this.interOpEdgeThroughput = new HashMap<>();
		this.edgeMap = new HashMap<>();
		this.triggerPeriod = triggerPeriod;

		init();

		this.waitTimeout = waitTimeout;
		this.numRetries = numRetries;
		this.maxParallelism = maxParallelism;
		this.currentPlacementAction = new ArrayList<>();
		this.nVertices = executionGraph.getTotalNumberOfVertices();
		this.suggestedPlacementAction = new ArrayList<>();
	}

	public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
		checkNotNull(resourceManagerGateway);
		this.resourceManagerGateway = resourceManagerGateway;
		populateResourceInformation(this.resourceManagerGateway);
		waitingForResourceManager = false;
	}

	private void populateResourceInformation(ResourceManagerGateway resourceManagerGateway) {
		CompletableFuture<Collection<TaskManagerInfo>> tmInfoFuture = resourceManagerGateway.requestTaskManagerInfo(
			Time.seconds(10));
		tmInfoFuture.thenAccept(tmInfos -> tmInfos.forEach(tmInfo -> {
			for (int i = 0; i < tmInfo.getNumberSlots(); i++) {
				TaskManagerLocation tmLoc = null;
				try {
					String externalAddress = tmInfo.getAddress().split("@")[1].split(":")[0];
					tmLoc = TaskManagerLocation.fromUnresolvedLocation(new UnresolvedTaskManagerLocation(
						tmInfo.getResourceId(),
						externalAddress,
						tmInfo.getDataPort()));
				} catch (UnknownHostException e) {
					log.error(
						"Error when deriving TaskManagerLocation : {} ",
						e.getMessage(),
						e);
				}
				taskManLocSlotCountMap.put(
					tmInfo.getResourceId().getResourceIdString(),
					new Tuple2<>(tmLoc, tmInfo.getNumberSlots()));
			}
		})).thenRun(this::initClusterInfo);
	}

	private void initClusterInfo() {
		taskManLocSlotCountMap.forEach((tmLocResourceId, tmLocNumSlotsTuple) -> log.info(
			"TaskManager with ID {} available at {} using data port {}",
			tmLocResourceId,
			tmLocNumSlotsTuple.f0.address().getHostAddress(),
			tmLocNumSlotsTuple.f0.dataPort()));

		//TODO Currently using the same CPU layout across the cluster, which is wrong!
		List<TaskManagerLocation> tmList = taskManLocSlotCountMap
			.values()
			.stream()
			.map(tuple -> tuple.f0)
			.collect(Collectors.toList());

		this.schedulingCluster = new SchedulingCluster(
			tmList,
			this.cpuLayout,
			this.maxParallelism,
			this.taskPerCore,
			log);
		taskManLocSlotCountMap.forEach((tmResourceId, tmLocNumSlotsTuple) -> {
			TaskManagerLocation tmLoc = tmLocNumSlotsTuple.f0;
			for (int i = 0; i < tmLocNumSlotsTuple.f1; i++) {
				schedulingCluster.addTaskSlot(new SimpleSlotInfo(tmResourceId, tmLoc, i));
				nSchedulingSlots++;
			}
		});
		taskManLocSlotCountMap
			.values()
			.stream()
			.map(tuple -> tuple.f0.address().getHostAddress())
			.distinct()
			.forEach(nodeAddress -> {
				List<Integer> cpuIdList = getCpuIdList(taskPerCore, cpuLayout);
				for (Integer cpuId : cpuIdList) {
					String cpuIdString =
						nodeAddress + ":" + cpuLayout.socketId(cpuId) + ":" + cpuId;
					schedulingCluster.addCpu(cpuIdString);
				}
			});
		try {
			onResourceInitialization();
			schedulingStrategy.setTopLevelContainer(schedulingCluster);
		} catch (Throwable e) {
			log.error("Unexpected error: " + e.getMessage(), e);
		}
	}

	private List<Integer> getCpuIdList(boolean taskPerCore, CpuLayout cpuLayout) {
		List<Integer> cpuIdList = new ArrayList<>();
		if (taskPerCore) {
			List<Integer> visitedCoreIdList = new ArrayList<>();
			for (int cpuId = 0; cpuId < cpuLayout.cpus(); cpuId++) {
				int currentCoreId = cpuLayout.socketId(cpuId) * cpuLayout.coresPerSocket()
					* cpuLayout.threadsPerCore() + cpuLayout.coreId(cpuId);
				if (!visitedCoreIdList.contains(currentCoreId)) {
					cpuIdList.add(cpuId);
					visitedCoreIdList.add(currentCoreId);
				}
			}
		} else {
			for (int cpuId = 0; cpuId < cpuLayout.cpus(); cpuId++) {
				cpuIdList.add(cpuId);
			}
		}
		return cpuIdList;
	}

	protected abstract void onResourceInitialization();

	protected void init() {
		AtomicInteger operatorCount = new AtomicInteger(0);
		schedulingTopology.getVertices().forEach(schedulingExecutionVertex -> {
			orderedOperatorMap.put(
				new Tuple3<>(
					schedulingExecutionVertex.getId(),
					schedulingExecutionVertex.getTaskName(),
					schedulingExecutionVertex.getSubTaskIndex()),
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
						log.debug("Created ExecutionEdge with ID: " + dee.getExecutionEdgeId());
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
		List<String> nodeList = schedulingCluster
			.getSubContainers()
			.stream()
			.map(SchedulingExecutionContainer::getId)
			.collect(
				Collectors.toList());
		return influxDBMetricsClient.getCpuUsageMetrics(nProcessingUnits, nodeList);
	}

	protected Map<String, Double> getCpuFrequencyMetrics() {
		List<String> nodeList = schedulingCluster
			.getSubContainers()
			.stream()
			.map(SchedulingExecutionContainer::getId)
			.collect(
				Collectors.toList());
		return influxDBMetricsClient.getCpuFrequencyMetrics(nProcessingUnits, nodeList);
	}

	protected Map<String, Double> getOperatorUsageMetrics() {
		return influxDBMetricsClient.getOperatorUsageMetrics();
	}

	protected void updateStateInformation() {
		Map<String, Tuple2<String, Integer>> currentOpPlacementInfo = influxDBMetricsClient.getOperatorPlacementMetrics();
		Map<String, Double> currentInterOpEdgeThroughput = influxDBMetricsClient.getRateMetricsFor(
			"taskmanager_job_task_edge_numRecordsProcessedPerSecond",
			"edge_id",
			"rate");
		if (!waitingForResourceManager) {
			Map<String, SchedulingExecutionVertex> currentOperators = StreamSupport
				.stream(schedulingTopology.getVertices().spliterator(), false)
				.collect(Collectors.toMap(
					vertex -> vertex.getId().toString(),
					vertex -> vertex));
			if (!currentOpPlacementInfo.isEmpty()) {
				schedulingCluster.releaseAllExecutionVertices();
				currentOpPlacementInfo
					.entrySet()
					.stream()
					.filter(placementInfoEntry -> currentOperators.containsKey(placementInfoEntry.getKey()))
					.forEach(placementInfoEntry -> {
						String operatorId = placementInfoEntry.getKey();
						Tuple2<String, Integer> tmIdCpuIdTuple = placementInfoEntry.getValue();
						TaskManagerLocation tmLoc = null;
						Tuple2<TaskManagerLocation, Integer> taskManSlotCount = taskManLocSlotCountMap
							.get(
								tmIdCpuIdTuple.f0);
						if (taskManSlotCount != null) {
							tmLoc = taskManSlotCount.f0;
						}
						Integer cpuId = tmIdCpuIdTuple.f1;
						Integer socketId = cpuLayout.socketId(cpuId);
						SchedulingExecutionVertex vertex = currentOperators.get(operatorId);
						if (tmLoc != null && cpuId >= 0 && socketId >= 0 && vertex != null) {
							schedulingCluster.forceSchedule(
								vertex,
								new Tuple3<>(tmLoc, cpuId, socketId));
						} else {
							log.warn(
								"Could not update placement information for operator {} running on task "
									+ "manager at {}, with CPU ID {}, running on socket ID {}",
								operatorId,
								tmLoc,
								cpuId,
								socketId);
						}
					});
			}
			Map<String, Double> operatorUsageMetrics = getOperatorUsageMetrics();
			Map<String, Double> cpuUsageMetrics = getCpuUsageMetrics();
			Map<String, Double> cpuFrequencyMetrics = getCpuFrequencyMetrics();
			schedulingCluster.updateResourceUsage(
				SchedulingExecutionContainer.CPU,
				cpuUsageMetrics);
			schedulingCluster.updateResourceUsage(
				SchedulingExecutionContainer.FREQ,
				cpuFrequencyMetrics);
			schedulingCluster.updateResourceUsage(
				SchedulingExecutionContainer.OPERATOR,
				operatorUsageMetrics);
		}
		Map<String, Double> filteredFlowRates = currentInterOpEdgeThroughput
			.entrySet()
			.stream()
			.filter(mapEntry -> edgeMap.containsKey(mapEntry.getKey()))
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		interOpEdgeThroughput.putAll(filteredFlowRates);
		if (previousRescheduleFuture != null) {
			updateMaxEdgeThroughputMatrices(interOpEdgeThroughput);
		}
		Map<SchedulingExecutionEdge, Double> edgeThroughputMap = interOpEdgeThroughput
			.entrySet()
			.stream()
			.collect(Collectors.toMap(
				mapEntry -> edgeMap.get(mapEntry.getKey()),
				Map.Entry::getValue))
			.entrySet()
			.stream()
			.collect(Collectors.toMap(Map.Entry::getKey, entry -> {
				ExecutionPlacement srcExecPlacement = entry
					.getKey()
					.getSourceSchedulingExecutionVertex()
					.getExecutionPlacement();
				ExecutionPlacement destExecPlacement = entry
					.getKey()
					.getTargetSchedulingExecutionVertex()
					.getExecutionPlacement();
				if (srcExecPlacement.getTaskManagerLocation() != null
					&& destExecPlacement.getTaskManagerLocation() != null) {
					String srcIp = srcExecPlacement
						.getTaskManagerLocation()
						.address()
						.getHostAddress();
					String destIp = destExecPlacement
						.getTaskManagerLocation()
						.address()
						.getHostAddress();
					if (!srcIp.equals(destIp)) {
						return entry.getValue() * INTER_NODE_TRAFFIC_SCALING_FACTOR;
					} else {
						int srcSocket = srcExecPlacement.getSocketId();
						int destSocket = destExecPlacement.getSocketId();
						if (srcSocket != destSocket) {
							return entry.getValue() * INTER_SOCKET_TRAFFIC_SCALING_FACTOR;
						}
					}
				}
				return entry.getValue();
			}));
		orderedEdgeList = edgeThroughputMap
			.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
			.map(Map.Entry::getKey).collect(Collectors.toList());
		mostRecentArrivalRate = influxDBMetricsClient.getMostRecentArrivalRate(sourceVertices
			.stream()
			.map(SchedulingExecutionVertex::getId)
			.map(ExecutionVertexID::toString)
			.collect(Collectors.toList()));
		logCurrentStatusInformation();
	}

	private String getCpuIdAsString(Tuple3<TaskManagerLocation, Integer, Integer> cpuIdTuple) {
		if (cpuIdTuple != null) {
			String tmAddress = "127.0.0.1";
			if (cpuIdTuple.f0 != null) {
				tmAddress = cpuIdTuple.f0.getHostname();
			}
			return tmAddress + SchedulingExecutionContainer.CPU_ID_DELIMITER + cpuIdTuple.f2
				+ SchedulingExecutionContainer.CPU_ID_DELIMITER + cpuIdTuple.f1;
		}
		return NULL_CPU_ID;
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
			log.info(
				"Shutting down update thread for {} scheduling mode",
				executionGraph.getScheduleMode());
			updateExecutor.cancel(true);
		}
		influxDBMetricsClient.closeConnection();
	}

	protected abstract void updatePlacementSolution();

	protected void updateCurrentPlacementInformation() {
		Map<SchedulingExecutionVertex, Tuple3<TaskManagerLocation, Integer, Integer>> cpuAssignmentMap = getTopLevelContainer()
			.getCurrentCpuAssignment();

		Map<Integer, Tuple3<TaskManagerLocation, Integer, Integer>> currentPlacementTemp = new HashMap<>();
		if (cpuAssignmentMap != null && !cpuAssignmentMap.isEmpty()) {
			log.info(
				"nVertices : {}, CPU Assignment map size {}",
				nVertices,
				cpuAssignmentMap.size());
			cpuAssignmentMap.forEach((executionVertex, assignment) ->
				log.info(
					"Execution Vertex {} assigned to cpu {} [socket {}] at {}",
					executionVertex.getTaskName() + ":" + executionVertex.getSubTaskIndex(),
					assignment.f1,
					assignment.f2,
					assignment.f0 == null ? "null" : assignment.f0.address().getHostAddress()));
			AtomicInteger vertexCount = new AtomicInteger(1);
			IterableUtils
				.toStream(schedulingTopology.getVertices())
				.forEachOrdered(schedulingExecutionVertex -> {
					Tuple3<TaskManagerLocation, Integer, Integer> currentVertexAssignment = cpuAssignmentMap
						.get(schedulingExecutionVertex);
					if (currentVertexAssignment == null) {
						log.warn(
							"Cannot find CPU assignment for vertex {}",
							getVertexName(schedulingExecutionVertex));
						currentVertexAssignment = getTopLevelContainer().scheduleVertex(
							schedulingExecutionVertex);
						log.warn(
							"Vertex {} arbitrarily scheduled at {}:{}:{}",
							getVertexName(schedulingExecutionVertex),
							currentVertexAssignment.f0 != null ? currentVertexAssignment.f0
								.address()
								.getHostAddress() : "null",
							currentVertexAssignment.f2,
							currentVertexAssignment.f1);
					}
					currentPlacementTemp.put(
						vertexCount.getAndIncrement(), currentVertexAssignment);
				});
		} else {
			log.warn("Could not retrieve current CPU assignment for this job.");
		}

		if (!currentPlacementTemp.isEmpty()) {
			currentPlacementAction = new ArrayList<>(currentPlacementTemp.values());
		} else {
			log.warn("Current placement is empty. CPU assignment map : {} ", cpuAssignmentMap);
		}
	}

	@Override
	public boolean isValidPlacementAction(List<Tuple3<TaskManagerLocation, Integer, Integer>> suggestedPlacementAction) {
		List<Integer> suggestedCpuIds = suggestedPlacementAction
			.stream()
			.map(placementInfo -> placementInfo.f1)
			.collect(Collectors.toList());
		return suggestedCpuIds.size() == nVertices
			&& suggestedCpuIds
			.stream()
			.noneMatch(procUnitId -> procUnitId < -1 || procUnitId > (nProcessingUnits - 1));
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
						if (!attempt.isFinished() && attempt.getState() != ExecutionState.CREATED) {
							try {
								Thread.sleep(waitTimeout);
							} catch (InterruptedException exception) {
								log.warn(
									"Thread waiting on halting of task {} was interrupted due to : {}",
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
					log.error("Couldn't halt execution for task {}", taskNameWithSubtaskIndex);
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
				try {
					if (executionGraph.getState() == JobStatus.RUNNING) {
						schedulingStrategy.startScheduling(this);
					}
				} catch (Exception e) {
					log.error("Unexpected error : {}", e.getMessage(), e);
				}
			}
		});
	}

	public void logPlacementAction(
		Tuple2<Integer, Integer> actionId,
		List<Tuple3<TaskManagerLocation, Integer, Integer>> placementAction) {
		if (placementAction != null && !placementAction.isEmpty()) {
			StringBuilder logMessage = new StringBuilder();
			for (int i = 0; i < placementAction.size(); i++) {
				Tuple3<TaskManagerLocation, Integer, Integer> currentOpPlacement = placementAction.get(
					i);
				logMessage
					.append("[")
					.append(i)
					.append("->")
					.append(currentOpPlacement.f0.address().getHostAddress())
					.append(":")
					.append(currentOpPlacement.f2)
					.append(":")
					.append(currentOpPlacement.f1)
					.append("], ");
			}
			log.info("Placement action for state ID {} is {} ", actionId, logMessage);
		}
	}

	private static class SimpleSlotInfo implements SlotInfo {
		private final AllocationID allocationID;
		private final String taskManagerResourceId;
		private final TaskManagerLocation taskManagerLocation;
		private final int physicalSlotNumber;

		SimpleSlotInfo(
			String taskManagerResourceId,
			TaskManagerLocation taskManagerLocation,
			int physicalSlotNumber) {
			this.allocationID = new AllocationID();
			this.taskManagerResourceId = taskManagerResourceId;
			this.taskManagerLocation = taskManagerLocation;
			this.physicalSlotNumber = physicalSlotNumber;
		}

		@Override
		public AllocationID getAllocationId() {
			return allocationID;
		}

		@Override
		public TaskManagerLocation getTaskManagerLocation() {
			return taskManagerLocation;
		}

		@Override
		public int getPhysicalSlotNumber() {
			return physicalSlotNumber;
		}

		@Override
		public ResourceProfile getResourceProfile() {
			return null;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			SimpleSlotInfo that = (SimpleSlotInfo) o;
			return physicalSlotNumber == that.physicalSlotNumber && taskManagerResourceId.equals(
				that.taskManagerResourceId) && taskManagerLocation.equals(that.taskManagerLocation);
		}

		@Override
		public int hashCode() {
			return Objects.hash(taskManagerResourceId, taskManagerLocation, physicalSlotNumber);
		}
	}
}
