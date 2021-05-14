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

	Tuple3<TaskManagerLocation, Integer, Integer> NULL_PLACEMENT = new Tuple3<>(
		null,
		-1,
		-1);

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
	Tuple3<TaskManagerLocation, Integer, Integer> scheduleVertex(SchedulingExecutionVertex schedulingExecutionVertex);

	/**
	 * @param schedulingExecutionVertex the execution vertex to be scheduled
	 *
	 * @return a {@link Tuple3} consisting of the TaskManagerLocation, CPU ID, and socket ID
	 */
	Tuple3<TaskManagerLocation, Integer, Integer> scheduleVertex(
		SchedulingExecutionVertex schedulingExecutionVertex,
		TaskManagerLocation targetTaskMan,
		Integer targetSocket);

	/**
	 * @param sourceVertex execution vertex that acts as the source of a stream edge
	 * @param targetVertex execution vertex that acts as the target/sink of the considered stream edge
	 *
	 * @return a list of {@link Tuple3} objects that include the task manager location, CPU ID,
	 * 	and socket ID in the case of a successful schedule
	 */
	List<Tuple3<TaskManagerLocation, Integer, Integer>> tryScheduleInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex);

	/**
	 * @param schedulingExecutionVertex execution vertex to be released
	 */
	void releaseExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex);

	/**
	 * Releases all execution vertices of the container and its sub-containers
	 */
	void releaseAllExecutionVertices();

	/**
	 * @param schedulingExecutionVertex the {@link SchedulingExecutionVertex} to be checked for assignment
	 *
	 * @return true if the vertex is scheduled, or false if not
	 */
	boolean isAssignedToContainer(SchedulingExecutionVertex schedulingExecutionVertex);

	/**
	 * @param schedulingExecutionVertex the {@link SchedulingExecutionVertex} to be scheduled
	 * @param cpuId a tuple that uniquely identifies a CPU
	 *
	 * Forcefully schedules a {@link SchedulingExecutionVertex} to the specified node, socket and cpu ID
	 *
	 * @return the {@link SchedulingExecutionVertex} that was already scheduled in that CPU, or null if
	 * the CPU was not running any vertex at the time for forceful scheduling
	 */
	SchedulingExecutionVertex forceSchedule(
		SchedulingExecutionVertex schedulingExecutionVertex,
		Tuple3<TaskManagerLocation, Integer, Integer> cpuId);

	/**
	 * @return the remaining number of slots
	 */
	int getRemainingCapacity();

	/**
	 * @param type the type of resource usage information needed, which should be one of the
	 * predefined string constants
	 *
	 * @return the amount of resources used as a double value
	 */
	double getResourceUsage(String type);

	/**
	 * @param type the type of resource usage to be updated, which should be one of the
	 * 	* predefined string constants
	 * @param resourceUsageMetrics the <CPU ID, value> pairs that need to be updated
	 */
	void updateResourceUsage(String type, Map<String, Double> resourceUsageMetrics);

	/**
	 * @return the ID of the {@link SchedulingExecutionContainer} as a String
	 */
	String getId();

	/**
	 * @return the current assignment of each {@link SchedulingExecutionVertex} in this
	 * {@link SchedulingExecutionContainer}
	 */
	Map<SchedulingExecutionVertex, Tuple3<TaskManagerLocation, Integer, Integer>> getCurrentCpuAssignment();

	/**
	 * @return the status of the {@link SchedulingExecutionContainer} as a String
	 */
	String getStatus();

	/**
	 * @param cpuIdFQN the fully qualified name of the CPU as an ID
	 *
	 * @return the CPU ID derived from the FQN of the CPU
	 */
	static int getCpuIdFromFQN(String cpuIdFQN) {
		String[] idParts = cpuIdFQN.split(CPU_ID_DELIMITER);
		if (idParts.length == 3) {
			return Integer.parseInt(idParts[2]);
		}
		return -1;
	}
}
