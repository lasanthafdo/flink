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

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionContainer;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;

import net.openhft.affinity.CpuLayout;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Container class for SchedulingNodes.
 */
public class SchedulingCluster implements SchedulingExecutionContainer {
	private static final String DEFAULT_CLUSTER_ID = "DEFAULT_CLUSTER_ID";

	private final Map<String, SchedulingExecutionContainer> nodes;
	private final Map<String, Integer> slotCount;
	private final List<TaskManagerLocation> taskManagerLocations;
	private final CpuLayout cpuLayout;
	private final Integer maxParallelism;
	private final boolean taskPerCore;
	private final Logger log;

	public SchedulingCluster(
		Collection<TaskManagerLocation> taskManagerLocations,
		CpuLayout cpuLayout,
		Integer maxParallelism,
		boolean taskPerCore,
		Logger log) {
		this.nodes = new HashMap<>();
		this.taskManagerLocations = new ArrayList<>();
		this.taskManagerLocations.addAll(taskManagerLocations);
		this.slotCount = new HashMap<>();
		this.cpuLayout = cpuLayout;
		this.maxParallelism = maxParallelism;
		this.taskPerCore = taskPerCore;
		this.log = log;
	}

	@Override
	public List<SchedulingExecutionContainer> getSubContainers() {
		return new ArrayList<>(nodes.values());
	}

	@Override
	public void addCpu(String cpuIdString) {
		String[] cpuIdParts = cpuIdString.split(":");
		if (cpuIdParts.length < 3) {
			log.error("Invalid CPU ID : " + cpuIdString);
			throw new FlinkRuntimeException("Invalid CPU ID : " + cpuIdString);
		}
		String nodeIp = cpuIdParts[0];
		if (!nodes.containsKey(nodeIp)) {
			nodes.put(nodeIp, new SchedulingNode(nodeIp, cpuLayout, maxParallelism, log));
			slotCount.put(nodeIp, 0);
		}
		nodes.get(nodeIp).addCpu(cpuIdString);
	}

	@Override
	public void addTaskSlot(SlotInfo slotInfo) {
		checkArgument(
			taskManagerLocations.contains(slotInfo.getTaskManagerLocation()),
			"Slot should belong to one of the registered task managers for the job");
		String nodeIp = slotInfo.getTaskManagerLocation().address().getHostAddress();
		if (!nodes.containsKey(nodeIp)) {
			nodes.put(nodeIp, new SchedulingNode(nodeIp, cpuLayout, maxParallelism, log));
			slotCount.put(nodeIp, 0);
		}
		nodes.get(nodeIp).addTaskSlot(slotInfo);
		slotCount.put(nodeIp, slotCount.get(nodeIp) + 1);
	}

	@Override
	public Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer> scheduleVertex(
		SchedulingExecutionVertex schedulingExecutionVertex) {
		Optional<SchedulingExecutionContainer> targetNode = nodes.values()
			.stream().filter(node -> {
				boolean hasRemainingSlots = node.getRemainingCapacity() >= 1;
				long opReplicationCount = node
					.getCurrentCpuAssignment()
					.keySet()
					.stream()
					.filter(scheduledVertex -> scheduledVertex
						.getTaskName()
						.equals(schedulingExecutionVertex.getTaskName()))
					.count();
				boolean maxParallelismReached = opReplicationCount >= slotCount.get(node.getId());
				return hasRemainingSlots && !maxParallelismReached;
			})
			.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR)));
		Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer> vertexAssignment = NULL_PLACEMENT;
		if (targetNode.isPresent()) {
			vertexAssignment = targetNode.get().scheduleVertex(schedulingExecutionVertex);
		} else {
			log.warn(
				"Could not find available node to schedule vertex {}",
				schedulingExecutionVertex.getTaskName() + ":"
					+ schedulingExecutionVertex.getSubTaskIndex());
		}
		return vertexAssignment;
	}

	@Override
	public Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer> scheduleVertex(
		SchedulingExecutionVertex schedulingExecutionVertex,
		TaskManagerLocation targetTaskManager,
		Integer targetSocket) {
		Optional<SchedulingExecutionContainer> targetNode = nodes
			.values()
			.stream().filter(node -> {
				boolean hasRemainingSlots = node.getRemainingCapacity() >= 1;
				long opReplicationCount = node
					.getCurrentCpuAssignment()
					.keySet()
					.stream()
					.filter(scheduledVertex -> scheduledVertex
						.getTaskName()
						.equals(schedulingExecutionVertex.getTaskName()))
					.count();
				boolean maxParallelismReached = opReplicationCount >= slotCount.get(node.getId());
				return hasRemainingSlots && !maxParallelismReached;
			})
			.filter(node -> node.getId().equals(targetTaskManager.address().getHostAddress()))
			.findFirst();
		return targetNode
			.map(sec -> sec.scheduleVertex(
				schedulingExecutionVertex,
				targetTaskManager,
				targetSocket))
			.orElse(SchedulingExecutionContainer.NULL_PLACEMENT);
	}

	@Override
	public List<Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer>> tryScheduleInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex) {

		//Try to schedule in a socket with two available CPU slots
		Optional<SchedulingExecutionContainer> firstPreferenceTargetNode = nodes.values()
			.stream().filter(node -> {
				boolean hasRemainingSlots = node.getRemainingCapacity() >= 2;
				long srcReplicationCount = node
					.getCurrentCpuAssignment()
					.keySet()
					.stream()
					.filter(scheduledVertex -> scheduledVertex
						.getTaskName()
						.equals(sourceVertex.getTaskName()))
					.count();
				long targetReplicationCount = node
					.getCurrentCpuAssignment()
					.keySet()
					.stream()
					.filter(scheduledVertex -> scheduledVertex
						.getTaskName()
						.equals(sourceVertex.getTaskName()))
					.count();
				boolean maxParallelismReached = srcReplicationCount >= slotCount.get(node.getId())
					|| targetReplicationCount >= slotCount.get(node.getId());
				return hasRemainingSlots && !maxParallelismReached;
			})
			.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR)));
		List<Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer>> tmLocCpuIdPairList = new ArrayList<>();
		if (firstPreferenceTargetNode.isPresent()) {
			SchedulingExecutionContainer node = firstPreferenceTargetNode.get();
			tmLocCpuIdPairList.addAll(node.tryScheduleInSameContainer(sourceVertex, targetVertex));
		}
		return tmLocCpuIdPairList;
	}

	@Override
	public void releaseExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex) {
		getSubContainers().forEach(node -> node.releaseExecutionVertex(schedulingExecutionVertex));
	}

	@Override
	public void releaseAllExecutionVertices() {
		if (log.isDebugEnabled()) {
			log.debug("Node status: {}", getStatus());
		}
		nodes.values().forEach(SchedulingExecutionContainer::releaseAllExecutionVertices);
	}

	@Override
	public boolean isAssignedToContainer(SchedulingExecutionVertex schedulingExecutionVertex) {
		for (SchedulingExecutionContainer node : nodes.values()) {
			if (node.isAssignedToContainer(schedulingExecutionVertex)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public SchedulingExecutionVertex forceSchedule(
		SchedulingExecutionVertex schedulingExecutionVertex,
		Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer> cpuId) {
		String nodeIp = cpuId.f0.address().getHostAddress();
		SchedulingExecutionVertex evictedVertex = nodes
			.get(nodeIp)
			.forceSchedule(schedulingExecutionVertex, cpuId);
		if (evictedVertex != null) {
			releaseExecutionVertex(evictedVertex);
		}
		return evictedVertex;
	}

	@Override
	public int getRemainingCapacity() {
		int capacity = 0;
		for (SchedulingExecutionContainer schedulingExecutionContainer : nodes.values()) {
			capacity += schedulingExecutionContainer.getRemainingCapacity();
		}
		return capacity;
	}

	@Override
	public double getResourceUsage(String type) {
		return nodes.values()
			.stream()
			.mapToDouble(node -> node.getResourceUsage(type))
			.average()
			.orElse(0d);
	}

	@Override
	public void updateResourceUsage(String type, Map<String, Double> resourceUsageMetrics) {
		if (CPU.equals(type) || FREQ.equals(type)) {
			Map<String, Map<String, Double>> resourceUsageMap = new HashMap<>();
			nodes.keySet().forEach(ip -> resourceUsageMap.put(ip, new HashMap<>()));
			resourceUsageMetrics.forEach((ipCpuId, val) -> {
				String[] ipCpuIdParts = ipCpuId.split(CPU_ID_DELIMITER);
				if (ipCpuIdParts.length >= 2 && resourceUsageMap.containsKey(ipCpuIdParts[0])) {
					resourceUsageMap.get(ipCpuIdParts[0]).put(ipCpuIdParts[1], val);
				}
			});
			resourceUsageMap.forEach((ip, resValMap) -> nodes
				.get(ip)
				.updateResourceUsage(type, resValMap));
		} else {
			nodes
				.values()
				.forEach(node -> node.updateResourceUsage(type, resourceUsageMetrics));
		}
	}

	@Override
	public String getId() {
		return DEFAULT_CLUSTER_ID;
	}

	@Override
	public Map<SchedulingExecutionVertex, Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer>> getCurrentCpuAssignment() {
		Map<SchedulingExecutionVertex, Tuple4<TaskManagerLocation, SlotSharingGroupId, Integer, Integer>> currentlyAssignedCpus = new HashMap<>();
		getSubContainers()
			.forEach(subContainer -> currentlyAssignedCpus.putAll(subContainer.getCurrentCpuAssignment()));
		return currentlyAssignedCpus;
	}

	@Override
	public String getStatus() {
		StringBuilder currentSchedulingStateMsg = new StringBuilder();
		currentSchedulingStateMsg
			.append("Cluster: (nProcUnits(Avail) : ").append(getRemainingCapacity())
			.append(", totCPU : ").append(getResourceUsage(CPU))
			.append(", operatorCPU :").append(getResourceUsage(OPERATOR))
			.append(") Nodes : [");
		nodes
			.values()
			.forEach(node -> currentSchedulingStateMsg
				.append(node.getStatus())
				.append(","));
		currentSchedulingStateMsg.append("]");
		return currentSchedulingStateMsg.toString();
	}
}
