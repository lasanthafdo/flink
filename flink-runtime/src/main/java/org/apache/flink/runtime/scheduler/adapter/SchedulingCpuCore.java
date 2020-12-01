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

	public SchedulingCpuCore(List<Integer> cpuIds) {
		checkNotNull(cpuIds);
		this.cpuAssignmentMap = new HashMap<>(cpuIds.size());
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
	public int getAvailableCapacity() {
		AtomicInteger integerCount = new AtomicInteger();
		cpuAssignmentMap
			.entrySet()
			.stream()
			.filter(entry -> entry.getValue() == null)
			.forEach(mapEntry -> integerCount.getAndIncrement());
		return integerCount.get();
	}
}
