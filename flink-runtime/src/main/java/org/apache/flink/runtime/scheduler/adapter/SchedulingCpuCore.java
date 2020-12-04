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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Container class for SchedulingCpuCores.
 */
public class SchedulingCpuCore implements SchedulingExecutionContainer {
	private final Map<Integer, SchedulingExecutionVertex> cpuAssignmentMap;
	private final Map<Integer, Double> cpuUsageMetrics;

	public SchedulingCpuCore(List<Integer> cpuIds) {
		checkNotNull(cpuIds);
		this.cpuAssignmentMap = new HashMap<>(cpuIds.size());
		this.cpuUsageMetrics = new HashMap<>(cpuIds.size());
		cpuIds.forEach(cpuId -> {
			this.cpuAssignmentMap.put(cpuId, null);
			this.cpuUsageMetrics.put(cpuId, 0.0);
		});
	}

	@Override
	public List<SchedulingExecutionContainer> getSubContainers() {
		return null;
	}

	@Override
	public int scheduleExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex) {
		int cpuId = cpuAssignmentMap
			.entrySet()
			.stream()
			.filter(mapEntry -> mapEntry.getValue() == null)
			.findFirst()
			.map(Map.Entry::getKey)
			.orElse(-1);
		if (cpuId != -1) {
			cpuAssignmentMap.put(cpuId, schedulingExecutionVertex);
		}
		return cpuId;
	}

	@Override
	public List<Integer> tryScheduleInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex) {

		List<Integer> cpuIdList = cpuAssignmentMap
			.entrySet()
			.stream()
			.filter(mapEntry -> mapEntry.getValue() == null)
			.map(Map.Entry::getKey).collect(Collectors.toList());
		if (cpuIdList.size() >= 2) {
			int sourceCpuId = cpuIdList.get(0);
			int targetCpuId = cpuIdList.get(1);
			cpuAssignmentMap.put(sourceCpuId, sourceVertex);
			cpuAssignmentMap.put(targetCpuId, targetVertex);

			cpuIdList.removeIf(id -> id != sourceCpuId && id != targetCpuId);
		}

		return cpuIdList;
	}

	@Override
	public int releaseExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex) {
		checkNotNull(schedulingExecutionVertex);
		int cpuId = cpuAssignmentMap
			.entrySet()
			.stream()
			.filter(entry -> schedulingExecutionVertex.equals(entry.getValue()))
			.findFirst()
			.map(Map.Entry::getKey)
			.orElse(-1);
		if (cpuId != -1) {
			cpuAssignmentMap.put(cpuId, null);
		}

		return cpuId;
	}

	@Override
	public void releaseAllExecutionVertices() {
		// Is this safe?? Concurrent modification?
		cpuAssignmentMap
			.keySet()
			.forEach(cpuAssignment -> cpuAssignmentMap.put(cpuAssignment, null));
	}

	@Override
	public boolean isAssignedToContainer(SchedulingExecutionVertex schedulingExecutionVertex) {
		return cpuAssignmentMap.containsValue(schedulingExecutionVertex);
	}

	@Override
	public int getAvailableCapacity() {
		AtomicInteger integerCount = new AtomicInteger();
		cpuAssignmentMap
			.entrySet()
			.stream()
			.filter(entry -> entry.getValue() == null)
			.forEach(mapEntry -> integerCount.getAndIncrement());
		return integerCount.get();
	}

	@Override
	public double getResourceUsage(String type) {
		if (SchedulingExecutionContainer.CPU.equals(type)) {
			return cpuUsageMetrics
				.values()
				.stream()
				.mapToDouble(Double::doubleValue)
				.average()
				.orElse(0d);
		} else {
			return 0d;
		}
	}

	@Override
	public void updateResourceUsageMetrics(String type, Map<Integer, Double> resourceUsageMetrics) {
		if (CPU.equals(type)) {
			cpuAssignmentMap
				.keySet()
				.forEach(cpuId -> cpuUsageMetrics.put(cpuId, resourceUsageMetrics.get(cpuId)));
		}
	}

	@Override
	public String getStatus() {
		StringBuilder currentStatusMsg = new StringBuilder();
		currentStatusMsg.append("(Available CPUs :").append(getAvailableCapacity())
			.append(", Resource Usage: ").append(getResourceUsage(CPU))
			.append(", Assignment:");
		cpuAssignmentMap.forEach((cpuId, vertex) -> {
			currentStatusMsg
				.append("[CPU ID: ").append(cpuId).append(", Vertex ID:");
			if (vertex != null) {
				currentStatusMsg
					.append(vertex.getId())
					.append(", Task Name: ")
					.append(vertex.getTaskName())
					.append("], ");
			} else {
				currentStatusMsg.append("Unassigned], ");
			}
		});

		return currentStatusMsg.toString();
	}
}
