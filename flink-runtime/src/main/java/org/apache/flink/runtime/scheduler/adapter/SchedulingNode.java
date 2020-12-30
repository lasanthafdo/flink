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

import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionContainer;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;

import net.openhft.affinity.CpuLayout;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Container class for SchedulingcpuSockets.
 */
public class SchedulingNode implements SchedulingExecutionContainer {
	private final Map<Integer, SchedulingExecutionContainer> cpuSockets;
	private final Map<Integer, Double> cpuUsageMetrics;
	private final Logger log;
	private final CpuLayout cpuLayout;

	public SchedulingNode(CpuLayout cpuLayout, Logger log) {
		this.cpuSockets = new HashMap<>();
		this.cpuUsageMetrics = new HashMap<>();
		this.log = log;
		this.cpuLayout = cpuLayout;
	}

	@Override
	public List<SchedulingExecutionContainer> getSubContainers() {
		return new ArrayList<>(cpuSockets.values());
	}

	@Override
	public void addCpu(int cpuId) {
		int socketId = cpuLayout.socketId(cpuId);
		if (!cpuSockets.containsKey(socketId)) {
			cpuSockets.put(socketId, new SchedulingCpuSocket(socketId, cpuLayout, log));
		}
		cpuSockets.get(socketId).addCpu(cpuId);
	}

	@Override
	public int scheduleExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex) {
		Optional<SchedulingExecutionContainer> targetSocket = cpuSockets.values()
			.stream().filter(cpuSocket -> cpuSocket.getRemainingCapacity() >= 1)
			.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR)));
		return targetSocket
			.map(schedulingExecutionContainer -> schedulingExecutionContainer.scheduleExecutionVertex(
				schedulingExecutionVertex))
			.orElse(-1);
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
	public List<Integer> tryScheduleInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex) {

		//Try to schedule in a socket with two available CPU slots
		Optional<SchedulingExecutionContainer> firstPreferenceTargetSocket = cpuSockets.values()
			.stream().filter(cpuSocket -> cpuSocket.getRemainingCapacity() >= 2)
			.min(Comparator.comparing(sec -> sec.getResourceUsage(OPERATOR)));
		List<Integer> cpuIds = new ArrayList<>();
		if (firstPreferenceTargetSocket.isPresent()) {
			SchedulingExecutionContainer cpuSocket = firstPreferenceTargetSocket.get();
			cpuIds.addAll(cpuSocket.tryScheduleInSameContainer(sourceVertex, targetVertex));
		}
		return cpuIds;
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
	public boolean forceSchedule(SchedulingExecutionVertex schedulingExecutionVertex, int cpuId) {
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
		cpuSockets.values().forEach(cpuSocket -> {
			cpuSocket.updateResourceUsageMetrics(type, resourceUsageMetrics);
		});
		if (CPU.equals(type)) {
			cpuUsageMetrics.putAll(resourceUsageMetrics.entrySet()
				.stream().collect(Collectors.toMap(
					entry -> Integer.parseInt(entry.getKey()), Map.Entry::getValue)));
		}
	}

	@Override
	public int getId() {
		return -1;
	}

	@Override
	public Map<SchedulingExecutionVertex, Integer> getCurrentCpuAssignment() {
		Map<SchedulingExecutionVertex, Integer> currentlyAssignedCpus = new HashMap<>();
		getSubContainers().forEach(subContainer -> {
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
		cpuSockets.values().forEach(cpuSocket -> {
			currentSchedulingStateMsg.append(cpuSocket.getStatus()).append(",");
		});
		currentSchedulingStateMsg.append("]}}");
		return currentSchedulingStateMsg.toString();
	}
}
