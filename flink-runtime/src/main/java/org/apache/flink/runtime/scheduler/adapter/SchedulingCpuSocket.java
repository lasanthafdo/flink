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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.agent.SchedulingAgentUtils.getVertexName;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Container class for SchedulingCpuCores.
 */
public class SchedulingCpuSocket implements SchedulingExecutionContainer {
	private final Map<Integer, Double> cpuUsageMetrics;
	private final Map<Integer, SchedulingExecutionVertex> cpuAssignmentMap;
	private final Map<Integer, Double> cpuFreqMap;

	private final Logger log;
	private final int socketId;
	private final CpuLayout cpuLayout;

	public SchedulingCpuSocket(int socketId, CpuLayout cpuLayout, Logger log) {
		this.cpuUsageMetrics = new HashMap<>();
		this.cpuAssignmentMap = new HashMap<>();
		this.cpuFreqMap = new HashMap<>();
		this.log = log;
		this.socketId = socketId;
		this.cpuLayout = cpuLayout;
	}

	@Override
	public List<SchedulingExecutionContainer> getSubContainers() {
		return null;
	}

	@Override
	public void addCpu(String cpuIdString) {
		int cpuId = SchedulingExecutionContainer.getCpuIdFromFQN(cpuIdString);
		checkState(cpuLayout.socketId(cpuId) == socketId);
		checkState(
			cpuAssignmentMap.size() < cpuLayout.threadsPerCore() * cpuLayout.coresPerSocket(),
			"Number of CPUs exceed number supported by socket");
		cpuAssignmentMap.putIfAbsent(cpuId, null);
		cpuFreqMap.putIfAbsent(cpuId, 0.0);
		cpuUsageMetrics.putIfAbsent(cpuId, 0.0);
	}

	@Override
	public void addTaskSlot(SlotInfo slotInfo) {
		log.debug(
			"addTaskSlot called for {} with allocation ID {} and IP {}",
			getClass().getCanonicalName(),
			slotInfo.getAllocationId().toString(),
			slotInfo.getTaskManagerLocation().address().getHostAddress());
		// Do nothing. We are not going to keep task manager information here
	}

	@Override
	public Tuple3<TaskManagerLocation, Integer, Integer> scheduleVertex(
		SchedulingExecutionVertex schedulingExecutionVertex) {
		int cpuId = cpuAssignmentMap
			.entrySet()
			.stream()
			.filter(mapEntry -> mapEntry.getValue() == null)
			.findFirst()
			.map(Map.Entry::getKey)
			.orElse(-1);
		if (cpuId != -1) {
			cpuAssignmentMap.put(cpuId, schedulingExecutionVertex);
			return new Tuple3<>(null, cpuId, socketId);
		} else {
			log.warn(
				"Could not find available CPU to schedule {}",
				schedulingExecutionVertex.getTaskName() + ":"
					+ schedulingExecutionVertex.getSubTaskIndex());
			return NULL_PLACEMENT;
		}
	}

	@Override
	public Tuple3<TaskManagerLocation, Integer, Integer> scheduleVertex(
		SchedulingExecutionVertex schedulingExecutionVertex,
		TaskManagerLocation targetTaskMan,
		Integer targetSocket) {
		checkArgument(
			targetSocket == this.socketId,
			"Scheduling Error : Target socket is different from current socket!");
		return scheduleVertex(schedulingExecutionVertex);
	}

	@Override
	public List<Tuple3<TaskManagerLocation, Integer, Integer>> tryScheduleInSameContainer(
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
		List<Tuple3<TaskManagerLocation, Integer, Integer>> tmLocCpuIdPairList = new ArrayList<>(
			cpuIdList.size());
		cpuIdList.forEach(cpuId -> tmLocCpuIdPairList.add(new Tuple3<>(null, cpuId, socketId)));
		return tmLocCpuIdPairList;
	}

	@Override
	public void releaseExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex) {
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
	public SchedulingExecutionVertex forceSchedule(
		SchedulingExecutionVertex schedulingExecutionVertex,
		Tuple3<TaskManagerLocation, Integer, Integer> cpuId) {
		if (cpuAssignmentMap.containsKey(cpuId.f1)) {
			SchedulingExecutionVertex currentlyAssignedVertex = cpuAssignmentMap.get(cpuId.f1);
			if (currentlyAssignedVertex != null) {
				log.warn(
					"Evicting currently scheduled execution vertex {} at CPU {} of {} to schedule {}",
					currentlyAssignedVertex.getTaskName() + ":"
						+ currentlyAssignedVertex.getSubTaskIndex(),
					cpuId.f1,
					cpuId.f0.address().getHostAddress(),
					schedulingExecutionVertex.getTaskName() + ":"
						+ schedulingExecutionVertex.getSubTaskIndex());
			}
			cpuAssignmentMap.put(cpuId.f1, schedulingExecutionVertex);
			return currentlyAssignedVertex;
		} else {
			throw new FlinkRuntimeException(
				"Invalid placement : Socket " + getId() + " of node " +
					(cpuId.f0 != null ? cpuId.f0.address().getHostAddress() : "null")
					+ " does not contain CPU ID " + cpuId.f1);
		}
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
		} else if (FREQ.equals(type)) {
			return cpuFreqMap
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
		} else if (FREQ.equals(type)) {
			cpuFreqMap
				.keySet()
				.forEach(cpuId -> cpuFreqMap.put(
					cpuId,
					resourceUsageMetrics.getOrDefault(String.valueOf(cpuId), 0d)));
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
	public String getId() {
		return String.valueOf(socketId);
	}

	@Override
	public Map<SchedulingExecutionVertex, Tuple3<TaskManagerLocation, Integer, Integer>> getCurrentCpuAssignment() {
		return cpuAssignmentMap
			.entrySet()
			.stream()
			.filter(entry -> entry.getValue() != null)
			.collect(Collectors.toMap(
				Map.Entry::getValue,
				entry -> new Tuple3<>(null, entry.getKey(), socketId)));
	}

	@Override
	public String getStatus() {
		StringBuilder currentStatusMsg = new StringBuilder();
		currentStatusMsg.append("Socket ").append(getId())
			.append(" : (nProcUnits(Avail) : ").append(getRemainingCapacity())
			.append(", totCPU : ").append(getResourceUsage(CPU))
			.append(", freqCPU : ").append(getResourceUsage(FREQ))
			.append(", operatorCPU : ").append(getResourceUsage(OPERATOR))
			.append(") Assignment : [");
		cpuAssignmentMap.forEach((cpuId, vertex) -> {
			currentStatusMsg
				.append("{CPU ID : ").append(cpuId)
				.append(", Vertex ID : ");
			if (vertex != null) {
				currentStatusMsg
					.append(vertex.getId())
					.append(", Task : ")
					.append(getVertexName(vertex))
					.append("},");
			} else {
				currentStatusMsg.append("Unassigned},");
			}
		});
		currentStatusMsg.append("]");

		return currentStatusMsg.toString();
	}
}
