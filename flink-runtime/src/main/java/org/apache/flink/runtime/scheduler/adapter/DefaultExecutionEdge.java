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

import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionEdge;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;

/**
 * Default implementation of {@link SchedulingExecutionVertex}.
 */
public class DefaultExecutionEdge implements SchedulingExecutionEdge {

	private final SchedulingExecutionVertex sourceVertex;
	private final SchedulingExecutionVertex targetVertex;
	private final SchedulingResultPartition resultPartition;

	public DefaultExecutionEdge(
		SchedulingExecutionVertex sourceVertex,
		SchedulingExecutionVertex targetVertex,
		SchedulingResultPartition resultPartition) {

		this.sourceVertex = sourceVertex;
		this.targetVertex = targetVertex;
		this.resultPartition = resultPartition;
	}

	@Override
	public SchedulingExecutionVertex getSourceSchedulingExecutionVertex() {
		return sourceVertex;
	}

	@Override
	public SchedulingExecutionVertex getTargetSchedulingExecutionVertex() {
		return targetVertex;
	}

	@Override
	public SchedulingResultPartition getSchedulingResultPartition() {
		return resultPartition;
	}

	public String getExecutionEdgeId() {
		return sourceVertex.getTaskName() + "_" + sourceVertex.getSubTaskIndex() + "@"
			+ resultPartition.getResultId() + "#" + targetVertex.getSubTaskIndex();
	}
}
