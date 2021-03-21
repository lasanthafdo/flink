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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.List;
import java.util.Map;

/**
 * Scheduling representation of {@link ExecutionVertex}.
 */
public interface SchedulingExecutionContainer {

	String CPU = "CPU";
	String OPERATOR = "OP";
	String FREQ = "FREQ";
	String CPU_ID_DELIMITER = ":";

	/**
	 * Returns the list of sub containers that belong to this container
	 * which are also of type {@link SchedulingExecutionContainer}
	 *
	 * @return {@link List} of type {@link SchedulingExecutionContainer}
	 */
	List<SchedulingExecutionContainer> getSubContainers();

	/**
	 * @param cpuIdString CPU ID consisting of taskManagerAddress, socket id, and CPU id
	 */
	void addCpu(String cpuIdString);

	/**
	 * @param slotInfo
	 */
	void addTaskSlot(SlotInfo slotInfo);

	/**
	 * @param schedulingExecutionVertex
	 *
	 * @return
	 */
	Tuple3<TaskManagerLocation, Integer, Integer> scheduleExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex);

	/**
	 * @param schedulingExecutionVertex
	 *
	 * @return
	 */
	@Deprecated
	int getCpuIdForScheduling(SchedulingExecutionVertex schedulingExecutionVertex);

	/**
	 * @param sourceVertex
	 * @param targetVertex
	 *
	 * @return
	 */
	List<Tuple3<TaskManagerLocation, Integer, Integer>> tryScheduleInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex);

	/**
	 * @param sourceVertex
	 * @param targetVertex
	 *
	 * @return
	 */
	@Deprecated
	List<Integer> getCpuIdsInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex);

	/**
	 * @param schedulingExecutionVertex
	 *
	 * @return
	 */
	int releaseExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex);

	/**
	 *
	 */
	void releaseAllExecutionVertices();

	boolean isAssignedToContainer(SchedulingExecutionVertex schedulingExecutionVertex);

	boolean forceSchedule(
		SchedulingExecutionVertex schedulingExecutionVertex,
		Tuple3<TaskManagerLocation, Integer, Integer> cpuId);

	int getRemainingCapacity();

	double getResourceUsage(String type);

	void updateResourceUsageMetrics(String type, Map<String, Double> resourceUsageMetrics);

	String getId();

	Map<SchedulingExecutionVertex, Tuple3<TaskManagerLocation, Integer, Integer>> getCurrentCpuAssignment();

	String getStatus();

	static int getCpuIdFromString(String cpuId) {
		String[] idParts = cpuId.split(CPU_ID_DELIMITER);
		if (idParts.length == 3) {
			return Integer.parseInt(idParts[2]);
		}
		return -1;
	}
}
