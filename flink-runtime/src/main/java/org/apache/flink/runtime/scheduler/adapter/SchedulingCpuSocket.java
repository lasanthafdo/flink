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
 * Container class for SchedulingCpuCores.
 */
public class SchedulingCpuSocket implements SchedulingExecutionContainer {
	private final List<SchedulingExecutionContainer> cpuCores;
	private final Map<Integer, Double> cpuUsageMetrics;
	private final Logger log;

	public SchedulingCpuSocket(List<SchedulingExecutionContainer> cpuCores, int nCpus, Logger log) {
		this.cpuCores = checkNotNull(cpuCores);
		this.cpuUsageMetrics = new HashMap<>(nCpus);
		this.log = log;
	}

	@Override
	public List<SchedulingExecutionContainer> getSubContainers() {
		return cpuCores;
	}

	@Override
	public int scheduleExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex) {
		Optional<SchedulingExecutionContainer> targetCore = cpuCores
			.stream()
			.max(Comparator.comparing(SchedulingExecutionContainer::getAvailableCapacity));
		return targetCore
			.map(schedulingExecutionContainer -> schedulingExecutionContainer.scheduleExecutionVertex(
				schedulingExecutionVertex))
			.orElse(-1);
	}

	@Override
	public List<Integer> tryScheduleInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex) {

		Optional<SchedulingExecutionContainer> targetCore = cpuCores
			.stream()
			.min(Comparator.comparing(sec -> sec.getResourceUsage(CPU)));
		List<Integer> cpuIds = new ArrayList<>();
		if (targetCore.isPresent()) {
			SchedulingExecutionContainer cpuCore = targetCore.get();
			log.info("Scheduling in target core with status : " + targetCore.get().getStatus());
			if (cpuCore.getAvailableCapacity() >= 2) {
				cpuIds.addAll(cpuCore.tryScheduleInSameContainer(sourceVertex, targetVertex));
			} else if (cpuCore.getAvailableCapacity() == 1) {
				cpuIds.add(cpuCore.scheduleExecutionVertex(sourceVertex));
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
		log.info("Socket status:\n {}", getStatus());
		cpuCores.forEach(SchedulingExecutionContainer::releaseAllExecutionVertices);
	}

	@Override
	public boolean isAssignedToContainer(SchedulingExecutionVertex schedulingExecutionVertex) {
		for (SchedulingExecutionContainer cpuCore : cpuCores) {
			if (cpuCore.isAssignedToContainer(schedulingExecutionVertex)) {
				return true;
			}
		}

		return false;
	}

	@Override
	public int getAvailableCapacity() {
		AtomicInteger integerCount = new AtomicInteger(0);
		cpuCores.forEach(schedulingExecutionContainer -> {
			integerCount.addAndGet(schedulingExecutionContainer.getAvailableCapacity());
		});
		return integerCount.get();
	}

	@Override
	public double getResourceUsage(String type) {
		return cpuCores
			.stream()
			.mapToDouble(cpuCore -> cpuCore.getResourceUsage(SchedulingExecutionContainer.CPU))
			.average()
			.orElse(0d);
	}

	@Override
	public void updateResourceUsageMetrics(String type, Map<Integer, Double> resourceUsageMetrics) {
		if (CPU.equals(type)) {
			cpuCores.forEach(cpuCore -> {
				cpuCore.updateResourceUsageMetrics(type, resourceUsageMetrics);
			});
			cpuUsageMetrics.putAll(resourceUsageMetrics);
		}
	}

	@Override
	public String getStatus() {
		StringBuilder currentSchedulingStateMsg = new StringBuilder();
		cpuCores.forEach(cpuCore -> {
			currentSchedulingStateMsg.append(cpuCore.getStatus()).append(",\t");
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
