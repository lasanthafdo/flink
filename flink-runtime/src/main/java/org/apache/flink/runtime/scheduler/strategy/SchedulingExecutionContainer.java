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
	 * @param slotInfo slot information that includes task manager location
	 */
	void addTaskSlot(SlotInfo slotInfo);

	/**
	 * @param schedulingExecutionVertex the execution vertex to be scheduled
	 *
	 * @return a {@link Tuple3} consisting of the TaskManagerLocation, CPU ID, and socket ID
	 */
	Tuple3<TaskManagerLocation, Integer, Integer> scheduleExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex);

	/**
	 * @param schedulingExecutionVertex the execution vertex for which the cpu ID is needed
	 *
	 * @return the CPU ID for the given execution vertex
	 */
	@Deprecated
	int getCpuIdForScheduling(SchedulingExecutionVertex schedulingExecutionVertex);

	/**
	 * @param sourceVertex execution vertex that acts as the source of a stream edge
	 * @param targetVertex execution vertex that acts as the target/sink of the considered stream edge
	 *
	 * @return a list of {@link Tuple3} objects that include the task manager location, CPU ID,
	 * and socket ID in the case of a successful schedule
	 */
	List<Tuple3<TaskManagerLocation, Integer, Integer>> tryScheduleInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex);

	/**
	 * @param sourceVertex execution vertex that acts as the source of a stream edge
	 * @param targetVertex execution vertex that acts as the target of a stream edge
	 *
	 * @return a list of CPU IDs as integers for the given execution vertices
	 */
	@Deprecated
	List<Integer> getCpuIdsInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex);

	/**
	 * @param schedulingExecutionVertex execution vertex to be released
	 *
	 * @return 0 on success and -1 on failure
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

	static int getCpuIdFromFQN(String cpuIdFQN) {
		String[] idParts = cpuIdFQN.split(CPU_ID_DELIMITER);
		if (idParts.length == 3) {
			return Integer.parseInt(idParts[2]);
		}
		return -1;
	}
}
