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

import net.openhft.affinity.CpuLayout;

import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionContainer;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

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
		Optional<SchedulingExecutionContainer> targetCore = cpuSockets.values()
			.stream()
			.min(Comparator.comparing(sec -> sec.getResourceUsage(CPU)));
		return targetCore
			.map(schedulingExecutionContainer -> schedulingExecutionContainer.scheduleExecutionVertex(
				schedulingExecutionVertex))
			.orElse(-1);
	}

	@Override
	public List<Integer> tryScheduleInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex) {

		Optional<SchedulingExecutionContainer> targetCore = cpuSockets.values()
			.stream()
			.min(Comparator.comparing(sec -> sec.getResourceUsage(CPU)));
		List<Integer> cpuIds = new ArrayList<>();
		if (targetCore.isPresent()) {
			SchedulingExecutionContainer cpuSocket = targetCore.get();
			log.info("Scheduling in target core with status : " + targetCore.get().getStatus());
			if (cpuSocket.getAvailableCapacity() >= 2) {
				cpuIds.addAll(cpuSocket.tryScheduleInSameContainer(sourceVertex, targetVertex));
			} else if (cpuSocket.getAvailableCapacity() == 1) {
				cpuIds.add(cpuSocket.scheduleExecutionVertex(sourceVertex));
			}
		}
		return cpuIds;
	}

	@Override
	public int releaseExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex) {
		return -1;
	}

	@Override
	public void releaseAllExecutionVertices() {
		log.info("Node status:\n {}", getStatus());
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
	public int getAvailableCapacity() {
		AtomicInteger integerCount = new AtomicInteger(0);
		cpuSockets.values().forEach(schedulingExecutionContainer -> {
			integerCount.addAndGet(schedulingExecutionContainer.getAvailableCapacity());
		});
		return integerCount.get();
	}

	@Override
	public double getResourceUsage(String type) {
		return cpuSockets.values()
			.stream()
			.mapToDouble(cpuSocket -> cpuSocket.getResourceUsage(SchedulingExecutionContainer.CPU))
			.average()
			.orElse(0d);
	}

	@Override
	public void updateResourceUsageMetrics(String type, Map<Integer, Double> resourceUsageMetrics) {
		if (CPU.equals(type)) {
			cpuSockets.values().forEach(cpuSocket -> {
				cpuSocket.updateResourceUsageMetrics(type, resourceUsageMetrics);
			});
			cpuUsageMetrics.putAll(resourceUsageMetrics);
		}
	}

	@Override
	public int getId() {
		return -1;
	}

	@Override
	public String getStatus() {
		StringBuilder currentSchedulingStateMsg = new StringBuilder();
		cpuSockets.values().forEach(cpuSocket -> {
			currentSchedulingStateMsg.append(cpuSocket.getStatus()).append(",\t");
		});
		currentSchedulingStateMsg
			.append("Overall (Available CPUs: ")
			.append(getAvailableCapacity())
			.append(", Resource Usage: ")
			.append(getResourceUsage(CPU))
			.append(").");
		return currentSchedulingStateMsg.toString();
	}
}
