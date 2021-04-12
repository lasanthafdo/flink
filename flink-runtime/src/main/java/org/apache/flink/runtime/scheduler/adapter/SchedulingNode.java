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

import net.openhft.affinity.CpuLayout;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Container class for SchedulingNodes.
 */
public class SchedulingNode implements SchedulingExecutionContainer {
	private final Map<Integer, SchedulingExecutionContainer> cpuSockets;
	private final Map<SlotInfo, List<SchedulingExecutionVertex>> slotAssignmentMap;
	private final String nodeIp;
	private final CpuLayout cpuLayout;
	private final Integer maxParallelism;
	private final Logger log;

	public SchedulingNode(String nodeIp, CpuLayout cpuLayout, Integer maxParallelism, Logger log) {
		this.cpuSockets = new HashMap<>();
		this.slotAssignmentMap = new HashMap<>();
		this.nodeIp = nodeIp;
		this.cpuLayout = cpuLayout;
		this.maxParallelism = maxParallelism;
		this.log = log;
	}

	@Override
	public List<SchedulingExecutionContainer> getSubContainers() {
		return new ArrayList<>(cpuSockets.values());
	}

	@Override
	public void addCpu(String cpuIdString) {
		int cpuId = SchedulingExecutionContainer.getCpuIdFromFQN(cpuIdString);
		int socketId = cpuLayout.socketId(cpuId);
		if (!cpuSockets.containsKey(socketId)) {
			cpuSockets.put(socketId, new SchedulingCpuSocket(socketId, cpuLayout, log));
		}
		cpuSockets.get(socketId).addCpu(cpuIdString);
	}

	@Override
	public void addTaskSlot(SlotInfo slotInfo) {
		checkArgument(slotInfo.getTaskManagerLocation().address().getHostAddress().equals(nodeIp));
		slotAssignmentMap.putIfAbsent(slotInfo, new ArrayList<>(maxParallelism));
	}

	@Override
	public Tuple3<TaskManagerLocation, Integer, Integer> scheduleVertex(
		SchedulingExecutionVertex schedulingExecutionVertex) {
		// TODO Find an optimal slot
		SlotInfo candidateSlot = slotAssignmentMap
			.entrySet()
			.stream()
			.filter(entry -> entry.getValue().size() < maxParallelism)    // Find a free slot
			.map(
				Map.Entry::getKey)
			.findAny().orElse(null);
		Tuple3<TaskManagerLocation, Integer, Integer> scheduledCpuInfo = NULL_PLACEMENT;
		if (candidateSlot != null) {
			SchedulingExecutionContainer targetSocket = cpuSockets.values()
				.stream().filter(cpuSocket -> cpuSocket.getRemainingCapacity() >= 1)
				.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR))).orElse(null);
			if (targetSocket != null) {
				scheduledCpuInfo = executeSocketScheduling(
					schedulingExecutionVertex,
					candidateSlot,
					targetSocket);
			}
		}
		return scheduledCpuInfo;
	}

	@Override
	public Tuple3<TaskManagerLocation, Integer, Integer> scheduleVertex(
		SchedulingExecutionVertex schedulingExecutionVertex,
		TaskManagerLocation targetTaskMan,
		Integer socketId) {
		SlotInfo candidateSlot = slotAssignmentMap
			.entrySet()
			.stream()
			.filter(entry -> entry.getValue().size() < maxParallelism)    // Find a free slot
			.filter(entry -> entry
				.getKey()
				.getTaskManagerLocation()
				.address()
				.equals(targetTaskMan.address()))
			.map(
				Map.Entry::getKey)
			.findAny().orElse(null);
		Tuple3<TaskManagerLocation, Integer, Integer> scheduledCpuInfo = NULL_PLACEMENT;
		if (candidateSlot != null) {
			SchedulingExecutionContainer targetSocket = cpuSockets.values()
				.stream().filter(cpuSocket -> cpuSocket.getRemainingCapacity() >= 1)
				.filter(cpuSocket -> Integer.parseInt(cpuSocket.getId()) == socketId)
				.findFirst().orElse(null);
			if (targetSocket != null) {
				scheduledCpuInfo = executeSocketScheduling(
					schedulingExecutionVertex,
					candidateSlot,
					targetSocket);
			}
		}
		return scheduledCpuInfo;
	}

	@NotNull
	private Tuple3<TaskManagerLocation, Integer, Integer> executeSocketScheduling(
		SchedulingExecutionVertex schedulingExecutionVertex,
		SlotInfo candidateSlot,
		SchedulingExecutionContainer targetSocket) {
		Tuple3<TaskManagerLocation, Integer, Integer> scheduledCpuInfo = targetSocket.scheduleVertex(
			schedulingExecutionVertex);
		if (scheduledCpuInfo != NULL_PLACEMENT) {
			scheduledCpuInfo.f0 = candidateSlot.getTaskManagerLocation();
			List<SchedulingExecutionVertex> assignedVertexList = slotAssignmentMap.get(
				candidateSlot);
			if (assignedVertexList != null && assignedVertexList.size() < maxParallelism) {
				assignedVertexList.add(schedulingExecutionVertex);
			} else {
				log.warn(
					"Attempt to assign execution vertex {} to non-existent slot {} "
						+ "or slot already contains the maximum allowed {} tasks",
					schedulingExecutionVertex.getTaskName() + ":"
						+ schedulingExecutionVertex.getSubTaskIndex(),
					candidateSlot.getTaskManagerLocation().address().getHostAddress() + ":"
						+ candidateSlot.getPhysicalSlotNumber(),
					maxParallelism);
			}
		}
		return scheduledCpuInfo;
	}

	@Override
	public List<Tuple3<TaskManagerLocation, Integer, Integer>> tryScheduleInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex) {

		//Try to schedule in a socket with two available CPU slots
		Optional<SchedulingExecutionContainer> firstPreferenceTargetSocket = cpuSockets.values()
			.stream().filter(cpuSocket -> cpuSocket.getRemainingCapacity() >= 2)
			.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR)));
		List<Tuple3<TaskManagerLocation, Integer, Integer>> tmLocCpuIdList = new ArrayList<>();
		if (firstPreferenceTargetSocket.isPresent()) {
			SchedulingExecutionContainer cpuSocket = firstPreferenceTargetSocket.get();
			List<Tuple3<TaskManagerLocation, Integer, Integer>> cpuIds = cpuSocket.tryScheduleInSameContainer(
				sourceVertex,
				targetVertex);
			// Try to find matching task slots and assign them
			for (int i = 0; i < cpuIds.size(); i++) {
				Tuple3<TaskManagerLocation, Integer, Integer> tuple = cpuIds.get(i);
				Integer cpuId = tuple.getField(1);
				Integer socketId = tuple.getField(2);
				Tuple3<TaskManagerLocation, Integer, Integer> tmLocCpuIdPair = new Tuple3<>(
					null,
					-1,
					-1);
				int cpuIdIndex = i;
				//TODO Find an optimal slot
				slotAssignmentMap
					.entrySet()
					.stream()
					.filter(entry -> entry.getValue().size() < maxParallelism)
					.map(
						Map.Entry::getKey)
					.findAny()
					.ifPresent(targetSlot -> {
						tmLocCpuIdPair.setFields(
							targetSlot.getTaskManagerLocation(),
							cpuId, socketId);
						SchedulingExecutionVertex selectedVertex = sourceVertex;
						if (cpuIdIndex != 0) {
							selectedVertex = targetVertex;
						}
						List<SchedulingExecutionVertex> targetSlotList = slotAssignmentMap.get(
							targetSlot);
						if (targetSlotList != null && targetSlotList.size() < maxParallelism) {
							targetSlotList.add(selectedVertex);
						} else {
							log.warn(
								"Attempt to assign execution vertex {} to non-existent slot {} "
									+ "or slot already contains the maximum allowed {} tasks",
								selectedVertex.getTaskName() + ":"
									+ selectedVertex.getSubTaskIndex(),
								targetSlot.getTaskManagerLocation().address().getHostAddress() + ":"
									+ targetSlot.getPhysicalSlotNumber(),
								maxParallelism);
						}
					});
				tmLocCpuIdList.add(tmLocCpuIdPair);
			}
		}
		return tmLocCpuIdList;
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
		cpuSockets.values().forEach(SchedulingExecutionContainer::releaseAllExecutionVertices);
		slotAssignmentMap.values().forEach(List::clear);
	}

	@Override
	public boolean isAssignedToContainer(SchedulingExecutionVertex schedulingExecutionVertex) {
		for (SchedulingExecutionContainer cpuSocket : cpuSockets.values()) {
			if (cpuSocket.isAssignedToContainer(schedulingExecutionVertex)) {
				return true;
			}
		}

		return false;
	}

	private boolean verifyEqual(TaskManagerLocation tm1, TaskManagerLocation tm2) {
		if (tm1 == null && tm2 == null) {
			return true;
		}
		if (tm1 == null || tm2 == null) {
			return false;
		}
		return tm1.address().getHostAddress().equals(tm2.address().getHostAddress())
			&& tm1.dataPort() == tm2.dataPort();
	}

	@Override
	public boolean forceSchedule(
		SchedulingExecutionVertex schedulingExecutionVertex,
		Tuple3<TaskManagerLocation, Integer, Integer> cpuId) {
		for (SchedulingExecutionContainer subContainer : getSubContainers()) {
			if (subContainer.forceSchedule(schedulingExecutionVertex, cpuId)) {
				for (Map.Entry<SlotInfo, List<SchedulingExecutionVertex>> entry : slotAssignmentMap
					.entrySet()) {
					if (verifyEqual(entry.getKey().getTaskManagerLocation(), cpuId.f0)) {
						if (entry.getValue() == null) {
							List<SchedulingExecutionVertex> assignedExecutionVertices = new ArrayList<>(
								maxParallelism);
							assignedExecutionVertices.add(schedulingExecutionVertex);
							slotAssignmentMap.put(
								entry.getKey(), assignedExecutionVertices);
							return true;
						} else if (entry.getValue().size() < maxParallelism) {
							entry.getValue().add(schedulingExecutionVertex);
							return true;
						}
					}
				}
				// Reaching here means that non of the assignments above succeeded
				// So we need to release the resources and return false
				subContainer.releaseExecutionVertex(schedulingExecutionVertex);
			}
		}
		return false;
	}

	@Override
	public int getRemainingCapacity() {
		int capacity = 0;
		for (SchedulingExecutionContainer schedulingExecutionContainer : cpuSockets.values()) {
			capacity += schedulingExecutionContainer.getRemainingCapacity();
		}
		return capacity;
	}

	@Override
	public double getResourceUsage(String type) {
		return cpuSockets.values()
			.stream()
			.mapToDouble(cpuSocket -> cpuSocket.getResourceUsage(type))
			.average()
			.orElse(0d);
	}

	@Override
	public void updateResourceUsageMetrics(String type, Map<String, Double> resourceUsageMetrics) {
		cpuSockets
			.values()
			.forEach(cpuSocket -> cpuSocket.updateResourceUsageMetrics(type, resourceUsageMetrics));
	}

	@Override
	public String getId() {
		return nodeIp;
	}

	@Override
	public Map<SchedulingExecutionVertex, Tuple3<TaskManagerLocation, Integer, Integer>> getCurrentCpuAssignment() {
		Map<SchedulingExecutionVertex, Tuple3<TaskManagerLocation, Integer, Integer>> currentlyAssignedCpus = new HashMap<>();
		getSubContainers().forEach(subContainer -> {
			Map<SchedulingExecutionVertex, Tuple3<TaskManagerLocation, Integer, Integer>> socketAssignment = subContainer
				.getCurrentCpuAssignment();
			socketAssignment.forEach((sev, cpuIdTuple) -> slotAssignmentMap
				.entrySet()
				.stream()
				.filter(entry -> entry.getValue().contains(sev))
				.findFirst().ifPresent(slotAssignmentEntry -> {
					cpuIdTuple.f0 = slotAssignmentEntry.getKey().getTaskManagerLocation();
					socketAssignment.put(sev, cpuIdTuple);
				}));
			currentlyAssignedCpus.putAll(socketAssignment);
		});
		return currentlyAssignedCpus;
	}

	@Override
	public String getStatus() {
		StringBuilder currentSchedulingStateMsg = new StringBuilder();
		currentSchedulingStateMsg
			.append("{Node: {Available CPUs : ").append(getRemainingCapacity())
			.append(", Container CPU Usage : ").append(getResourceUsage(CPU))
			.append(", Operator CPU Usage :").append(getResourceUsage(OPERATOR))
			.append(", Sockets : [");
		cpuSockets
			.values()
			.forEach(cpuSocket -> currentSchedulingStateMsg
				.append(cpuSocket.getStatus())
				.append(","));
		currentSchedulingStateMsg.append("]}}");
		return currentSchedulingStateMsg.toString();
	}
}
