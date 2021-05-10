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

package org.apache.flink.runtime.scheduler.agent;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QActorCriticModelTest extends TestLogger {

	private QActorCriticModel qActorCriticModel;
	private int nProcUnitsPerSocket = 4;
	private int nVertices = 7;

	@Before
	public void setUp() {
		List<Tuple2<InetAddress, Integer>> nodeSocketCounts = generateNodeSocketInfo();
		qActorCriticModel = new QActorCriticModel(
			nodeSocketCounts,
			nVertices,
			nProcUnitsPerSocket,
			null);
	}

	private List<Tuple2<InetAddress, Integer>> generateNodeSocketInfo() {
		List<Tuple2<InetAddress, Integer>> nodeSocketCounts = new ArrayList<>();
		nodeSocketCounts.add(new Tuple2<>(InetAddress.getLoopbackAddress(), 2));
		return nodeSocketCounts;
	}

	@Test
	public void testStateActionSpaceGenerationSingleNode() {
		Map<Integer, Map<Integer, Long>> stateActionMap = qActorCriticModel.generateTopLevelActionSpace(
		);
		int expectedSize = 70;
		Assert.assertEquals(expectedSize, stateActionMap.size());
	}

	@Test
	public void testAltStateActionSpaceGenerationSingleNode() {
		Map<Integer, List<Integer>> stateActionMap = qActorCriticModel.generateStateActionSpace(
			nVertices,
			nProcUnitsPerSocket);
		int expectedSize = 70;
		Assert.assertEquals(expectedSize, stateActionMap.size());
	}

	@Test
	public void testAltStateActionSpaceGenerationMultiNode() {
		qActorCriticModel.addToSocketScheduleIdMap("10.38.205.112:0");
		qActorCriticModel.addToSocketScheduleIdMap("10.38.205.112:1");
		qActorCriticModel.addToSocketScheduleIdMap("10.38.205.61:0");
		qActorCriticModel.addToSocketScheduleIdMap("10.38.205.61:1");
		nVertices = 34;
		nProcUnitsPerSocket = 6;
		Map<Integer, List<Integer>> stateActionMap = qActorCriticModel.generateStateActionSpace(
			nVertices, nProcUnitsPerSocket);
		int expectedSize = 0;
		Assert.assertEquals(expectedSize, stateActionMap.size());
	}

	@Test
	public void testStateActionSpaceGenerationMultiNode() {
		qActorCriticModel.addToSocketScheduleIdMap("10.38.205.112:0");
		qActorCriticModel.addToSocketScheduleIdMap("10.38.205.112:1");
		qActorCriticModel.addToSocketScheduleIdMap("10.38.205.61:0");
		qActorCriticModel.addToSocketScheduleIdMap("10.38.205.61:1");
		nVertices = 34;
		nProcUnitsPerSocket = 6;
		Map<Integer, Map<Integer, Long>> stateActionMap = qActorCriticModel.generateTopLevelActionSpace(
		);
		int expectedSize = 0;
		Assert.assertEquals(expectedSize, stateActionMap.size());
	}

	@Test
	public void testCombinationGenerationMultiNode() {
		qActorCriticModel.addToSocketScheduleIdMap("10.38.205.112:0");
		qActorCriticModel.addToSocketScheduleIdMap("10.38.205.112:1");
		qActorCriticModel.addToSocketScheduleIdMap("10.38.205.61:0");
		qActorCriticModel.addToSocketScheduleIdMap("10.38.205.61:1");
		nVertices = 34;
		nProcUnitsPerSocket = 6;
		List<List<Integer>> combinationList = qActorCriticModel.generateCombinationsFor(
			nVertices, nProcUnitsPerSocket);
		int expectedSize = 0;
		Assert.assertEquals(expectedSize, combinationList.size());
	}

}
