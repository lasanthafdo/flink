package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionContainer;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;

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

	public SchedulingCpuSocket(List<SchedulingExecutionContainer> cpuCores, int nCpus) {
		this.cpuCores = checkNotNull(cpuCores);
		this.cpuUsageMetrics = new HashMap<>(nCpus);
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
}
