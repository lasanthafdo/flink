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

import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.List;
import java.util.Map;

/**
 * Scheduling representation of {@link ExecutionVertex}.
 */
public interface SchedulingExecutionContainer {

	String CPU = "CPU";
	String OPERATOR = "OP";

	List<SchedulingExecutionContainer> getSubContainers();

	void addCpu(int cpuId);

	int scheduleExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex);

	int getCpuIdForScheduling(SchedulingExecutionVertex schedulingExecutionVertex);

	List<Integer> tryScheduleInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex);

	List<Integer> getCpuIdsInSameContainer(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex);

	int releaseExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex);

	void releaseAllExecutionVertices();

	boolean isAssignedToContainer(SchedulingExecutionVertex schedulingExecutionVertex);

	boolean forceSchedule(SchedulingExecutionVertex schedulingExecutionVertex, int cpuId);

	int getRemainingCapacity();

	double getResourceUsage(String type);

	void updateResourceUsageMetrics(String type, Map<String, Double> resourceUsageMetrics);

	int getId();

	Map<SchedulingExecutionVertex, Integer> getCurrentCpuAssignment();

	String getStatus();
}
