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

import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;

/**
 * Default implementation of {@link SchedulingExecutionVertex}.
 */
public class PhysicalExecutionEdge {

	private final String sourceVertexId;
	private final String targetVertexId;
	private final Integer sourceCpuId;
	private final Integer targetCpuId;

	public PhysicalExecutionEdge(
		String sourceVertexId,
		String targetVertexId,
		Integer sourceCpuId,
		Integer targetCpuId) {

		this.sourceVertexId = sourceVertexId;
		this.targetVertexId = targetVertexId;
		this.sourceCpuId = sourceCpuId;
		this.targetCpuId = targetCpuId;
	}

	public String getPhysicalExecutionEdgeId() {
		return sourceVertexId + "@" + targetVertexId + "|"
			+ sourceCpuId + "@" + targetCpuId;
	}

	public String getSourceVertexId() {
		return sourceVertexId;
	}

	public String getTargetVertexId() {
		return targetVertexId;
	}

	public Integer getSourceCpuId() {
		return sourceCpuId;
	}

	public Integer getTargetCpuId() {
		return targetCpuId;
	}
}
