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

package org.apache.flink.runtime.execution;

import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.io.Serializable;

/**
 * Preferred task location for a given task set by the scheduler.
 */
public class ExecutionPlacement implements Serializable {

	private static final long serialVersionUID = -786800431659013391L;

	private final TaskManagerLocation taskManagerLocation;
	private final int cpuId;
	private final int socketId;

	public ExecutionPlacement(
		TaskManagerLocation taskManagerLocation,
		int cpuId, int socketId) {
		this.taskManagerLocation = taskManagerLocation;
		this.cpuId = cpuId;
		this.socketId = socketId;
	}

	public TaskManagerLocation getTaskManagerLocation() {
		return taskManagerLocation;
	}

	public int getCpuId() {
		return cpuId;
	}

	public int getSocketId() {
		return socketId;
	}
}
