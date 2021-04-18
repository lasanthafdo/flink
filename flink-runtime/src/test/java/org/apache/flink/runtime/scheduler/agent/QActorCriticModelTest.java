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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QActorCriticModelTest extends TestLogger {

	private QActorCriticModel qActorCriticModel;
	@Before
	public void setUp() {
		List<Tuple2<InetAddress, Integer>> nodeSocketCounts = generateNodeSocketInfo();
		qActorCriticModel = new QActorCriticModel(nodeSocketCounts, 7);
	}

	private List<Tuple2<InetAddress, Integer>> generateNodeSocketInfo() {
		List<Tuple2<InetAddress, Integer>> nodeSocketCounts = new ArrayList<>();
		nodeSocketCounts.add(new Tuple2<>(InetAddress.getLoopbackAddress(), 4));
		return nodeSocketCounts;
	}

	@Test
	public void testStateActionSpaceGeneration() {
		Map<Integer, List<Integer>> stateActionMap = qActorCriticModel.generateStateActionSpace(7);
		Map<Integer, List<Integer>> expected = new HashMap<>();
		Assert.assertEquals(expected, stateActionMap);
	}
}
