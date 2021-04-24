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

import org.apache.flink.api.java.tuple.Tuple3;
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
	private final List<TaskManagerLocation> taskManagerLocations;
	private final CpuLayout cpuLayout;
	private final Integer maxParallelism;
	private final Logger log;

	public SchedulingCluster(
		Collection<TaskManagerLocation> taskManagerLocations,
		CpuLayout cpuLayout,
		Integer maxParallelism,
		Logger log) {
		this.nodes = new HashMap<>();
		this.taskManagerLocations = new ArrayList<>();
		this.taskManagerLocations.addAll(taskManagerLocations);
		this.cpuLayout = cpuLayout;
		this.maxParallelism = maxParallelism;
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
		}
		nodes.get(nodeIp).addTaskSlot(slotInfo);
	}

	@Override
	public Tuple3<TaskManagerLocation, Integer, Integer> scheduleVertex(
		SchedulingExecutionVertex schedulingExecutionVertex) {
		Optional<SchedulingExecutionContainer> targetNode = nodes.values()
			.stream().filter(node -> node.getRemainingCapacity() >= 1)
			.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR)));
		Tuple3<TaskManagerLocation, Integer, Integer> vertexAssignment = NULL_PLACEMENT;
		if (targetNode.isPresent()) {
			vertexAssignment = targetNode.get().scheduleVertex(schedulingExecutionVertex);
		} else {
			log.warn("Could not find available node to schedule vertex {}",
				schedulingExecutionVertex.getTaskName() + ":"
					+ schedulingExecutionVertex.getSubTaskIndex());
		}
		return vertexAssignment;
	}

	@Override
	public Tuple3<TaskManagerLocation, Integer, Integer> scheduleVertex(
		SchedulingExecutionVertex schedulingExecutionVertex,
		TaskManagerLocation targetTaskMan,
		Integer targetSocket) {
		Optional<SchedulingExecutionContainer> targetNode = nodes
			.values()
			.stream().filter(node -> node.getRemainingCapacity() >= 1)
			.filter(node -> node.getId().equals(targetTaskMan.address().getHostAddress()))
			.findFirst();
		return targetNode
			.map(sec -> sec.scheduleVertex(
				schedulingExecutionVertex,
				targetTaskMan,
				targetSocket))
			.orElse(SchedulingExecutionContainer.NULL_PLACEMENT);
	}

	@Override
	public List<Tuple3<TaskManagerLocation, Integer, Integer>> tryScheduleInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex) {

		//Try to schedule in a socket with two available CPU slots
		Optional<SchedulingExecutionContainer> firstPreferenceTargetNode = nodes.values()
			.stream().filter(node -> node.getRemainingCapacity() >= 2)
			.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR)));
		List<Tuple3<TaskManagerLocation, Integer, Integer>> tmLocCpuIdPairList = new ArrayList<>();
		if (firstPreferenceTargetNode.isPresent()) {
			SchedulingExecutionContainer node = firstPreferenceTargetNode.get();
			tmLocCpuIdPairList.addAll(node.tryScheduleInSameContainer(sourceVertex, targetVertex));
		}
		return tmLocCpuIdPairList;
	}

	@Override
	public void releaseExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex) {
		// Not implemented
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
	public boolean forceSchedule(
		SchedulingExecutionVertex schedulingExecutionVertex,
		Tuple3<TaskManagerLocation, Integer, Integer> cpuId) {
		for (SchedulingExecutionContainer subContainer : getSubContainers()) {
			if (subContainer.forceSchedule(schedulingExecutionVertex, cpuId)) {
				return true;
			}
		}
		return false;
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
	public void updateResourceUsageMetrics(String type, Map<String, Double> resourceUsageMetrics) {
		nodes
			.values()
			.forEach(node -> node.updateResourceUsageMetrics(type, resourceUsageMetrics));
	}

	@Override
	public String getId() {
		return DEFAULT_CLUSTER_ID;
	}

	@Override
	public Map<SchedulingExecutionVertex, Tuple3<TaskManagerLocation, Integer, Integer>> getCurrentCpuAssignment() {
		Map<SchedulingExecutionVertex, Tuple3<TaskManagerLocation, Integer, Integer>> currentlyAssignedCpus = new HashMap<>();
		getSubContainers()
			.forEach(subContainer -> currentlyAssignedCpus.putAll(subContainer.getCurrentCpuAssignment()));
		return currentlyAssignedCpus;
	}

	@Override
	public String getStatus() {
		StringBuilder currentSchedulingStateMsg = new StringBuilder();
		currentSchedulingStateMsg
			.append("{Cluster: {Available CPUs : ").append(getRemainingCapacity())
			.append(", Container CPU Usage : ").append(getResourceUsage(CPU))
			.append(", Operator CPU Usage :").append(getResourceUsage(OPERATOR))
			.append(", Nodes : [");
		nodes
			.values()
			.forEach(node -> currentSchedulingStateMsg
				.append(node.getStatus())
				.append(","));
		currentSchedulingStateMsg.append("]}}");
		return currentSchedulingStateMsg.toString();
	}
}
