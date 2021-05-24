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

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionContainer;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;

import net.openhft.affinity.CpuLayout;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Container class for SchedulingNodes.
 */
public class SchedulingNode implements SchedulingExecutionContainer {
	private final Map<Integer, SchedulingExecutionContainer> cpuSockets;
	private final Map<SlotInfo, Set<SchedulingExecutionVertex>> slotAssignmentMap;
	private final Map<SlotInfo, SlotSharingGroup> slotSharingGroupMap;
	private final String nodeIp;
	private final CpuLayout cpuLayout;
	private final Integer maxOperatorsInMultiTaskSlot;
	private final Logger log;

	public SchedulingNode(
		String nodeIp,
		CpuLayout cpuLayout,
		Integer maxOperatorsInMultiTaskSlot,
		Logger log) {
		this.cpuSockets = new HashMap<>();
		this.slotAssignmentMap = new HashMap<>();
		this.slotSharingGroupMap = new HashMap<>();
		this.nodeIp = nodeIp;
		this.cpuLayout = cpuLayout;
		this.maxOperatorsInMultiTaskSlot = maxOperatorsInMultiTaskSlot;
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
		slotAssignmentMap.putIfAbsent(slotInfo, new HashSet<>(maxOperatorsInMultiTaskSlot));
	}

	@Override
	public Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer> scheduleVertex(
		SchedulingExecutionVertex schedulingExecutionVertex) {
		// TODO Find an optimal slot
		SlotInfo candidateSlot = slotAssignmentMap
			.entrySet()
			.stream()
			.filter(entry -> { // Find a free slot
				boolean hasVertexWithSameJobVertexId = entry
					.getValue()
					.stream()
					.anyMatch(slotVertex -> slotVertex
						.getId()
						.getJobVertexId()
						.equals(schedulingExecutionVertex.getId().getJobVertexId()));
				return !hasVertexWithSameJobVertexId
					&& entry.getValue().size() < maxOperatorsInMultiTaskSlot;
			})
			.map(
				Map.Entry::getKey)
			.findAny().orElse(null);
		Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer> scheduledCpuInfo = NULL_PLACEMENT;
		if (candidateSlot != null) {
			SchedulingExecutionContainer targetSocket = cpuSockets.values()
				.stream().filter(cpuSocket -> cpuSocket.getRemainingCapacity() >= 1)
				.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR))).orElse(null);
			if (targetSocket != null) {
				scheduledCpuInfo = executeSocketScheduling(
					schedulingExecutionVertex,
					candidateSlot,
					targetSocket);
			} else {
				log.warn(
					"Could not find suitable CPU socket to schedule {}",
					schedulingExecutionVertex.getTaskName() + ":"
						+ schedulingExecutionVertex.getSubTaskIndex());
			}
		}
		return scheduledCpuInfo;
	}

	@Override
	public Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer> scheduleVertex(
		SchedulingExecutionVertex schedulingExecutionVertex,
		TaskManagerLocation targetTaskManager,
		Integer targetSocketId) {
		SlotInfo candidateSlot = slotAssignmentMap
			.entrySet()
			.stream()
			.filter(entry -> {  // Find a free slot
				boolean isTargetedTaskManager = entry
					.getKey()
					.getTaskManagerLocation()
					.address()
					.equals(targetTaskManager.address());
				boolean hasVertexWithSameJobVertexId = entry
					.getValue()
					.stream()
					.anyMatch(slotVertex -> slotVertex
						.getId()
						.getJobVertexId()
						.equals(schedulingExecutionVertex.getId().getJobVertexId()));
				return isTargetedTaskManager && !hasVertexWithSameJobVertexId
					&& entry.getValue().size() < maxOperatorsInMultiTaskSlot;
			})
			.map(
				Map.Entry::getKey)
			.findAny().orElse(null);
		Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer> scheduledCpuInfo = NULL_PLACEMENT;
		if (candidateSlot != null) {
			SchedulingExecutionContainer targetSocket = cpuSockets.values()
				.stream().filter(cpuSocket -> cpuSocket.getRemainingCapacity() >= 1)
				.filter(cpuSocket -> Integer.parseInt(cpuSocket.getId()) == targetSocketId)
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
	private Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer> executeSocketScheduling(
		SchedulingExecutionVertex schedulingExecutionVertex,
		SlotInfo candidateSlot,
		SchedulingExecutionContainer targetSocket) {
		Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer> scheduledCpuInfo = targetSocket
			.scheduleVertex(
				schedulingExecutionVertex);
		if (scheduledCpuInfo != NULL_PLACEMENT) {
			scheduledCpuInfo.f0 = candidateSlot.getTaskManagerLocation();
			SlotSharingGroup slotSharingGroup = slotSharingGroupMap.computeIfAbsent(
				candidateSlot,
				slotInfo -> new SlotSharingGroup());
			slotSharingGroup.addVertexToGroup(
				schedulingExecutionVertex.getId().getJobVertexId(),
				ResourceSpec.DEFAULT);
			scheduledCpuInfo.f1 = slotSharingGroup;
			addVertexToSlot(candidateSlot, schedulingExecutionVertex);
		} else {
			log.warn(
				"Scheduling {} on target socket {} failed",
				schedulingExecutionVertex.getTaskName() + ":"
					+ schedulingExecutionVertex.getSubTaskIndex(),
				targetSocket.getId());
		}
		return scheduledCpuInfo;
	}

	@Override
	public List<Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer>> tryScheduleInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex) {

		//Try to schedule in a socket with two available CPU slots
		Optional<SchedulingExecutionContainer> firstPreferenceTargetSocket = cpuSockets.values()
			.stream().filter(cpuSocket -> cpuSocket.getRemainingCapacity() >= 2)
			.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR)));
		List<Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer>> vertexPlacementInfoList = new ArrayList<>();
		if (firstPreferenceTargetSocket.isPresent()) {
			SchedulingExecutionContainer cpuSocket = firstPreferenceTargetSocket.get();
			List<Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer>> connectedVerticesPlacementInfos =
				cpuSocket.tryScheduleInSameContainer(sourceVertex, targetVertex);
			// Try to find matching task slots and assign them
			for (int i = 0; i < connectedVerticesPlacementInfos.size(); i++) {
				Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer> tuple = connectedVerticesPlacementInfos
					.get(i);
				Integer cpuId = tuple.getField(1);
				Integer socketId = tuple.getField(2);
				Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer> vertexPlacementInfo = NULL_PLACEMENT;
				int cpuIdIndex = i;
				//TODO Find an optimal slot
				slotAssignmentMap
					.entrySet()
					.stream()
					.filter(entry -> {
						boolean hasVertexWithSameJobVertexId = entry
							.getValue()
							.stream()
							.anyMatch(slotVertex -> slotVertex
								.getId()
								.getJobVertexId()
								.equals(sourceVertex.getId().getJobVertexId()));
						return !hasVertexWithSameJobVertexId
							&& entry.getValue().size() < maxOperatorsInMultiTaskSlot;
					})
					.map(
						Map.Entry::getKey)
					.findAny()
					.ifPresent(targetSlot -> {
						final SchedulingExecutionVertex selectedVertex;
						if (cpuIdIndex == 0) {
							selectedVertex = sourceVertex;
						} else {
							selectedVertex = targetVertex;
						}
						SlotSharingGroup slotSharingGroup = slotSharingGroupMap.computeIfAbsent(
							targetSlot, slotInfo -> new SlotSharingGroup());
						slotSharingGroup.addVertexToGroup(
							selectedVertex.getId().getJobVertexId(),
							ResourceSpec.DEFAULT);
						vertexPlacementInfo.setFields(
							targetSlot.getTaskManagerLocation(),
							slotSharingGroup,
							cpuId, socketId);
						addVertexToSlot(targetSlot, selectedVertex);
					});
				vertexPlacementInfoList.add(vertexPlacementInfo);
			}
		}
		return vertexPlacementInfoList;
	}

	private void addVertexToSlot(SlotInfo targetSlot, SchedulingExecutionVertex selectedVertex) {
		Set<SchedulingExecutionVertex> targetSlotList = slotAssignmentMap.get(
			targetSlot);
		if (targetSlotList != null && targetSlotList.size() < maxOperatorsInMultiTaskSlot) {
			targetSlotList.add(selectedVertex);
		} else {
			log.warn(
				"Attempt to assign execution vertex {} to non-existent slot {} "
					+ "or slot already contains the maximum allowed {} tasks",
				selectedVertex.getTaskName() + ":"
					+ selectedVertex.getSubTaskIndex(),
				targetSlot.getTaskManagerLocation().address().getHostAddress() + ":"
					+ targetSlot.getPhysicalSlotNumber(),
				maxOperatorsInMultiTaskSlot);
		}
	}

	@Override
	public void releaseExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex) {
		Optional<Map.Entry<SlotInfo, Set<SchedulingExecutionVertex>>> optionalSlotAssignmentEntry = slotAssignmentMap
			.entrySet()
			.stream()
			.filter(slotInfoAndVertexSet -> slotInfoAndVertexSet.getValue() != null
				&& slotInfoAndVertexSet
				.getValue()
				.contains(schedulingExecutionVertex))
			.findAny();
		if (optionalSlotAssignmentEntry.isPresent()) {
			Set<SchedulingExecutionVertex> slotVerticesSet = optionalSlotAssignmentEntry
				.get()
				.getValue();
			slotVerticesSet.remove(schedulingExecutionVertex);
			if (slotVerticesSet.isEmpty()) {
				SlotInfo slotInfo = optionalSlotAssignmentEntry.get().getKey();
				slotSharingGroupMap.remove(slotInfo);
			}
		}
		getSubContainers()
			.forEach(socket -> socket.releaseExecutionVertex(schedulingExecutionVertex));
	}

	@Override
	public void releaseAllExecutionVertices() {
		if (log.isDebugEnabled()) {
			log.debug("Node status: {}", getStatus());
		}
		cpuSockets.values().forEach(SchedulingExecutionContainer::releaseAllExecutionVertices);
		slotAssignmentMap.values().forEach(Set::clear);
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
	public SchedulingExecutionVertex forceSchedule(
		SchedulingExecutionVertex schedulingExecutionVertex,
		Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer> placementInfo) {
		boolean alreadyScheduledInNode = slotAssignmentMap
			.values()
			.stream()
			.anyMatch(vertexInSlotMap -> vertexInSlotMap.contains(schedulingExecutionVertex));
		if (alreadyScheduledInNode) {
			log.warn(
				"Vertex {} already scheduled in node {}",
				schedulingExecutionVertex.getTaskName() + ":"
					+ schedulingExecutionVertex.getSubTaskIndex(),
				getId());
		}
		SchedulingExecutionVertex evictedVertex = cpuSockets
			.get(placementInfo.f3)
			.forceSchedule(schedulingExecutionVertex, placementInfo);
		SlotInfo candidateSlot = slotSharingGroupMap
			.entrySet()
			.stream()
			.filter(entry -> entry.getValue().equals(placementInfo.f1))
			.map(
				Map.Entry::getKey)
			.findFirst()  // Would have avoided complexity if the IDE would shut up
			.orElseGet(() -> slotAssignmentMap
				.entrySet()
				.stream()
				.filter(slotInfoAndVertexSet -> {
					SlotInfo slotInfo = slotInfoAndVertexSet.getKey();
					Set<SchedulingExecutionVertex> assignedExecutionVertices = slotInfoAndVertexSet.getValue();
					boolean taskManagerLocMatch = verifyEqual(
						slotInfo.getTaskManagerLocation(),
						placementInfo.f0);
					boolean hasRemainingSingleSlots = assignedExecutionVertices == null
						|| assignedExecutionVertices.size() < maxOperatorsInMultiTaskSlot;
					return taskManagerLocMatch && hasRemainingSingleSlots;
				})
				.map(Map.Entry::getKey)
				.findFirst()
				.orElse(null));
		if (candidateSlot != null) {
			Set<SchedulingExecutionVertex> assignedExecutionVertices = slotAssignmentMap.computeIfAbsent(
				candidateSlot,
				slotInfo -> new HashSet<>(
					maxOperatorsInMultiTaskSlot));
			assignedExecutionVertices.add(schedulingExecutionVertex);
			slotSharingGroupMap.putIfAbsent(candidateSlot, placementInfo.f1);
			return evictedVertex;
		} else {
			// Reaching here means that non of the assignments above succeeded
			// So we need to release the resources and return false
			cpuSockets.get(placementInfo.f3).releaseExecutionVertex(schedulingExecutionVertex);
			throw new FlinkRuntimeException("Invalid placement : Could not schedule vertex "
				+ schedulingExecutionVertex.getTaskName() + ":"
				+ schedulingExecutionVertex.getSubTaskIndex() + " on CPU ID " + placementInfo.f2
				+ " of node "
				+ placementInfo.f0.address().getHostAddress()
			);
		}
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
	public void updateResourceUsage(String type, Map<String, Double> resourceUsageMetrics) {
		cpuSockets
			.values()
			.forEach(cpuSocket -> cpuSocket.updateResourceUsage(type, resourceUsageMetrics));
	}

	@Override
	public String getId() {
		return nodeIp;
	}

	@Override
	public Map<SchedulingExecutionVertex, Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer>> getCurrentCpuAssignment() {
		Map<SchedulingExecutionVertex, Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer>> currentlyAssignedCpus = new HashMap<>();
		getSubContainers().forEach(subContainer -> {
			Map<SchedulingExecutionVertex, Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer>> socketAssignment = subContainer
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
			.append("Node ")
			.append(getId())
			.append(" : (nProcUnits(Avail) : ")
			.append(getRemainingCapacity())
			.append(", totCPU : ")
			.append(getResourceUsage(CPU))
			.append(", operatorCPU :")
			.append(getResourceUsage(OPERATOR))
			.append(") Sockets : [");
		cpuSockets
			.values()
			.forEach(cpuSocket -> currentSchedulingStateMsg
				.append(cpuSocket.getStatus())
				.append(","));
		currentSchedulingStateMsg.append("]");
		return currentSchedulingStateMsg.toString();
	}
}
