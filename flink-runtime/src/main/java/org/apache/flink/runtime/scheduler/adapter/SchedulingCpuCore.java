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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Container class for SchedulingCpuCores.
 */
public class SchedulingCpuCore implements SchedulingExecutionContainer {
	private final Map<Integer, SchedulingExecutionVertex> cpuAssignmentMap;
	private final Map<Integer, Double> cpuUsageMetrics;
	private final int coreId;
	private final CpuLayout cpuLayout;
	private final Logger log;

	public SchedulingCpuCore(int coreId, CpuLayout cpuLayout, Logger log) {
		this.cpuAssignmentMap = new HashMap<>();
		this.cpuUsageMetrics = new HashMap<>();
		this.log = log;
		this.cpuLayout = cpuLayout;
		this.coreId = coreId;
	}

	@Override
	public List<SchedulingExecutionContainer> getSubContainers() {
		return null;
	}

	@Override
	public void addCpu(int cpuId) {
		checkState(cpuLayout.coreId(cpuId) == coreId);
		checkState(cpuAssignmentMap.size() < cpuLayout.threadsPerCore());
		cpuAssignmentMap.putIfAbsent(cpuId, null);
		cpuUsageMetrics.putIfAbsent(cpuId, 0.0);
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

		cpuAssignmentMap
			.keySet()
			.forEach(cpuAssignment -> cpuAssignmentMap.put(cpuAssignment, null));
	}

	@Override
	public boolean isAssignedToContainer(SchedulingExecutionVertex schedulingExecutionVertex) {
		return cpuAssignmentMap.containsValue(schedulingExecutionVertex);
	}

	@Override
	public int getRemainingCapacity() {
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
		if (CPU.equals(type)) {
			return cpuUsageMetrics
				.values()
				.stream()
				.mapToDouble(Double::doubleValue)
				.average()
				.orElse(0d);
		} else if (OPERATOR.equals(type)) {
			return cpuAssignmentMap
				.values()
				.stream()
				.filter(Objects::nonNull)
				.mapToDouble(SchedulingExecutionVertex::getCurrentCpuUsage)
				.sum();
		} else {
			return 0d;
		}
	}

	@Override
	public void updateResourceUsageMetrics(String type, Map<String, Double> resourceUsageMetrics) {
		if (CPU.equals(type)) {
			cpuAssignmentMap
				.keySet()
				.forEach(cpuId -> cpuUsageMetrics.put(
					cpuId,
					resourceUsageMetrics.get(String.valueOf(cpuId))));
		} else if (OPERATOR.equals(type)) {
			cpuAssignmentMap
				.values().stream().filter(Objects::nonNull)
				.forEach(schedulingExecutionVertex -> {
					Double operatorCpuUsage = resourceUsageMetrics.get(schedulingExecutionVertex
						.getId()
						.toString());
					if (operatorCpuUsage != null) {
						schedulingExecutionVertex.setCurrentCpuUsage(operatorCpuUsage);
					} else {
						schedulingExecutionVertex.setCurrentCpuUsage(0d);
					}
				});
		}
	}

	@Override
	public int getId() {
		return coreId;
	}

	@Override
	public String getStatus() {
		StringBuilder currentStatusMsg = new StringBuilder();
		currentStatusMsg.append("{Core : [{Core ID:").append(getId())
			.append("}, {Available CPUs : ").append(getRemainingCapacity())
			.append("}, {Container CPU Usage : ").append(getResourceUsage(CPU))
			.append("}, {Operator CPU Usage : ").append(getResourceUsage(OPERATOR))
			.append("}, {Assignment : [");
		cpuAssignmentMap.forEach((cpuId, vertex) -> {
			currentStatusMsg
				.append("{CPU : [{CPU ID : ").append(cpuId)
				.append("}, {Vertex ID : ");
			if (vertex != null) {
				currentStatusMsg
					.append(vertex.getId())
					.append("}, {Task Name : ")
					.append(vertex.getTaskName())
					.append("}]}, ");
			} else {
				currentStatusMsg.append("Unassigned}]}, ");
			}
		});
		currentStatusMsg.append("]}]}");

		return currentStatusMsg.toString();
	}
}
