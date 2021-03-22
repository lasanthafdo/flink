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
	private final Map<SlotInfo, SchedulingExecutionVertex> slotAssignmentMap;
	private final Logger log;
	private final CpuLayout cpuLayout;
	private final String nodeIp;

	public SchedulingNode(String nodeIp, CpuLayout cpuLayout, Logger log) {
		this.cpuSockets = new HashMap<>();
		this.slotAssignmentMap = new HashMap<>();
		this.nodeIp = nodeIp;
		this.cpuLayout = cpuLayout;
		this.log = log;
	}

	@Override
	public List<SchedulingExecutionContainer> getSubContainers() {
		return new ArrayList<>(cpuSockets.values());
	}

	@Override
	public void addCpu(String cpuIdString) {
		int cpuId = SchedulingExecutionContainer.getCpuIdFromString(cpuIdString);
		int socketId = cpuLayout.socketId(cpuId);
		if (!cpuSockets.containsKey(socketId)) {
			cpuSockets.put(socketId, new SchedulingCpuSocket(socketId, cpuLayout, log));
		}
		cpuSockets.get(socketId).addCpu(cpuIdString);
	}

	@Override
	public void addTaskSlot(SlotInfo slotInfo) {
		checkArgument(slotInfo.getTaskManagerLocation().address().getHostAddress().equals(nodeIp));
		slotAssignmentMap.putIfAbsent(slotInfo, null);
	}

	@Override
	public Tuple3<TaskManagerLocation, Integer, Integer> scheduleExecutionVertex(
		SchedulingExecutionVertex schedulingExecutionVertex) {
		SlotInfo candidateSlot = slotAssignmentMap
			.entrySet()
			.stream()
			.filter(entry -> entry.getValue() == null)
			.map(
				Map.Entry::getKey)
			.findAny().orElse(null);
		Tuple3<TaskManagerLocation, Integer, Integer> scheduledCpuInfo;
		if (candidateSlot != null) {
			Optional<SchedulingExecutionContainer> targetSocket = cpuSockets.values()
				.stream().filter(cpuSocket -> cpuSocket.getRemainingCapacity() >= 1)
				.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR)));
			scheduledCpuInfo = targetSocket
				.map(sec -> sec.scheduleExecutionVertex(schedulingExecutionVertex))
				.map(resultTuple -> {
					resultTuple.setField(candidateSlot.getTaskManagerLocation(), 0);
					return resultTuple;
				})
				.orElse(new Tuple3<>(null, -1, -1));
			if (scheduledCpuInfo.getField(0) != null) {
				slotAssignmentMap.put(candidateSlot, schedulingExecutionVertex);
			}
		} else {
			scheduledCpuInfo = new Tuple3<>(null, -1, -1);
		}
		return scheduledCpuInfo;
	}

	@Override
	public int getCpuIdForScheduling(SchedulingExecutionVertex schedulingExecutionVertex) {
		Optional<SchedulingExecutionContainer> targetSocket = cpuSockets.values()
			.stream().filter(cpuSocket -> cpuSocket.getRemainingCapacity() >= 1)
			.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR)));
		return targetSocket
			.map(schedulingExecutionContainer -> schedulingExecutionContainer.getCpuIdForScheduling(
				schedulingExecutionVertex))
			.orElse(-1);
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
				slotAssignmentMap
					.entrySet()
					.stream()
					.filter(entry -> entry.getValue() == null)
					.map(
						Map.Entry::getKey)
					.findAny()
					.ifPresent(targetSlot -> {
						tmLocCpuIdPair.setFields(
							targetSlot.getTaskManagerLocation(),
							cpuId, socketId);
						if (cpuIdIndex == 0) {
							slotAssignmentMap.put(targetSlot, sourceVertex);
						} else {
							slotAssignmentMap.put(targetSlot, targetVertex);
						}
					});
				tmLocCpuIdList.add(tmLocCpuIdPair);
			}
		}
		return tmLocCpuIdList;
	}

	@Override
	public List<Integer> getCpuIdsInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex) {
		Optional<SchedulingExecutionContainer> firstPreferenceTargetSocket = cpuSockets.values()
			.stream().filter(cpuSocket -> cpuSocket.getRemainingCapacity() >= 2)
			.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR)));
		List<Integer> cpuIds = new ArrayList<>();
		if (firstPreferenceTargetSocket.isPresent()) {
			SchedulingExecutionContainer cpuSocket = firstPreferenceTargetSocket.get();
			cpuIds.addAll(cpuSocket.getCpuIdsInSameContainer(sourceVertex, targetVertex));
		}
		return cpuIds;
	}

	@Override
	public int releaseExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex) {
		return -1;
	}

	@Override
	public void releaseAllExecutionVertices() {
		if (log.isDebugEnabled()) {
			log.debug("Node status: {}", getStatus());
		}
		cpuSockets.values().forEach(SchedulingExecutionContainer::releaseAllExecutionVertices);
		slotAssignmentMap.keySet().forEach(key -> slotAssignmentMap.put(key, null));
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

	@Override
	public boolean forceSchedule(
		SchedulingExecutionVertex schedulingExecutionVertex,
		Tuple3<TaskManagerLocation, Integer, Integer> cpuId) {
		for (SchedulingExecutionContainer subContainer : getSubContainers()) {
			if (subContainer.forceSchedule(schedulingExecutionVertex, cpuId)) {
				for (Map.Entry<SlotInfo, SchedulingExecutionVertex> entry : slotAssignmentMap
					.entrySet()) {
					if (entry.getValue() == null) {
						slotAssignmentMap.put(entry.getKey(), schedulingExecutionVertex);
						return true;
					}
				}
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
			Map<SchedulingExecutionVertex, Tuple3<TaskManagerLocation, Integer, Integer>> subConAssignment = subContainer
				.getCurrentCpuAssignment();
			subConAssignment.forEach((sev, cpuIdTuple) -> slotAssignmentMap
				.entrySet()
				.stream()
				.filter(entry -> sev.equals(entry.getValue()))
				.findFirst().ifPresent(slotAssignmentEntry -> {
					cpuIdTuple.setField(slotAssignmentEntry.getKey().getTaskManagerLocation(), 0);
					subConAssignment.put(sev, cpuIdTuple);
				}));
			currentlyAssignedCpus.putAll(subContainer.getCurrentCpuAssignment());
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
