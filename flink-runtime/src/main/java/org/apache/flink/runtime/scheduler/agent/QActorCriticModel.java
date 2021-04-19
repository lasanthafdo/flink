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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.apache.flink.shaded.guava18.com.google.common.collect.BiMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.HashBiMap;

import com.github.chen0040.rl.learning.actorcritic.ActorCriticLearner;
import org.paukov.combinatorics.CombinatoricsVector;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.paukov.combinatorics.CombinatoricsFactory.createSimpleCombinationGenerator;

/**
 * Wrapper class for the actor-critic training.
 */
public class QActorCriticModel {

	private static final String INET_ADDR_SOCKET_COUNT_DELIM = ":";
	private int stateCount;
	private int actionCount;
	private int previousStateId;

	private final ActorCriticLearner agent = new ActorCriticLearner(stateCount, actionCount);
	private final List<Transition> transitionList = new ArrayList<>();
	private InfluxDBTransitionsClient influxDBTransitionsClient;
	private final Map<Integer, List<Integer>> stateSpaceMap;
	private final BiMap<String, Integer> socketScheduleIdMap;
	private final int nSchedulingSocketSlots;
	private final Logger log;
	private int currentStateId;
	private int currentActionId;
	private final static String retentionPolicyName = "one_day";

	public QActorCriticModel(
		List<Tuple2<InetAddress, Integer>> nodeSocketCounts,
		int nVertices, int nCpusPerSocket,
		Logger log) {
		this.socketScheduleIdMap = HashBiMap.create();
		this.nSchedulingSocketSlots = getAvailableSlotsAfterDerivation(nodeSocketCounts);
		this.stateSpaceMap = generateStateActionSpace(nVertices, nCpusPerSocket);
		this.stateCount = stateSpaceMap.size();
		this.actionCount = stateSpaceMap.size();
		this.log = log;

		setupInfluxDBConnection();
	}

	@VisibleForTesting
	QActorCriticModel(
		List<Tuple2<InetAddress, Integer>> nodeSocketCounts,
		int nVertices, int nCpusPerSocket) {
		this.socketScheduleIdMap = HashBiMap.create();
		this.nSchedulingSocketSlots = getAvailableSlotsAfterDerivation(nodeSocketCounts);
		this.stateSpaceMap = generateStateActionSpace(nVertices, nCpusPerSocket);
		this.stateCount = stateSpaceMap.size();
		this.actionCount = stateSpaceMap.size();
		this.log = null;
	}

	private String getSocketAddress(InetAddress node, Integer socket) {
		return node.getHostAddress() + INET_ADDR_SOCKET_COUNT_DELIM + socket;
	}

	private Tuple2<InetAddress, Integer> getSocketId(String socketAddress) throws UnknownHostException {
		String[] socketIdParts = socketAddress.split(INET_ADDR_SOCKET_COUNT_DELIM);
		return new Tuple2<>(
			InetAddress.getByName(socketIdParts[0]),
			Integer.valueOf(socketIdParts[1]));
	}

	private int getAvailableSlotsAfterDerivation(List<Tuple2<InetAddress, Integer>> nodeSocketCounts) {
		AtomicInteger scheduleIdCount = new AtomicInteger(0);
		nodeSocketCounts.forEach(locSlotCount -> {
			for (int i = 0; i < locSlotCount.f1; i++) {
				socketScheduleIdMap.put(
					getSocketAddress(locSlotCount.f0, i),
					scheduleIdCount.incrementAndGet());
			}
		});
		return scheduleIdCount.get();
	}

	@VisibleForTesting
	void addToSocketScheduleIdMap(String socketAddress) {
		Integer currentMaxId = socketScheduleIdMap
			.values()
			.stream()
			.max(Comparator.naturalOrder())
			.orElse(0);
		socketScheduleIdMap.put(socketAddress, currentMaxId + 1);
	}

	@VisibleForTesting
	Map<Integer, List<Integer>> generateStateActionSpace(int nVertices, int nCpusPerSocket) {
		Map<Integer, List<Integer>> actionMap = new HashMap<>();
		CombinatoricsVector<Integer> slotIds = new CombinatoricsVector<>();
		socketScheduleIdMap.forEach((nodeSocketCountEntry, stateId) -> {
			for (int i = 0; i < nCpusPerSocket; i++) {
				//TODO Is this approach generating more states than necessary?
				slotIds.addValue(stateId);
			}
		});
		int actionId = 1;
		Generator<Integer> gen = createSimpleCombinationGenerator(slotIds, nVertices);
		for (ICombinatoricsVector<Integer> cpuSelection : gen.generateAllObjects()) {
			// Puts a state/action ID and state pair like <1, {3,1,1,4,2,2,1}>
			List<Integer> stateIdVector = cpuSelection.getVector();
			actionMap.put(actionId++, stateIdVector);
		}
		return actionMap;
	}

	public int getStateFor(List<Tuple3<TaskManagerLocation, Integer, Integer>> cpuAssignment) {
		List<Integer> cpuAssignmentStateVector = cpuAssignment
			.stream()
			.map(operatorLoc -> socketScheduleIdMap.get(getSocketAddress(
				operatorLoc.f0.address(),
				operatorLoc.f2)))
			.collect(Collectors.toList());
		return stateSpaceMap
			.entrySet()
			.stream()
			.filter(entry -> entry.getValue().size() == cpuAssignmentStateVector.size() && entry
				.getValue()
				.containsAll(cpuAssignmentStateVector))
			.map(
				Map.Entry::getKey)
			.findFirst()
			.orElse(-1);
	}

	static class Transition {
		int oldState;
		int newState;
		int action;
		double reward;

		public Transition(int oldState, int action, int newState, double reward) {
			this.oldState = oldState;
			this.newState = newState;
			this.reward = reward;
			this.action = action;
		}
	}

	private void setupInfluxDBConnection() {
		influxDBTransitionsClient = new InfluxDBTransitionsClient(
			"http://127.0.0.1:8086",
			"flink-transitions", retentionPolicyName, log);
		influxDBTransitionsClient.setup();
	}

	public List<Tuple2<InetAddress, Integer>> getPlacementSolution(int action) {
		List<Tuple2<InetAddress, Integer>> placementSolution = new ArrayList<>();
		List<Integer> placementAction = stateSpaceMap.get(action);
		placementAction.forEach(stateId -> {
			try {
				placementSolution.add(getSocketId(socketScheduleIdMap
					.inverse()
					.get(stateId)));
			} catch (UnknownHostException e) {
				log.warn(
					"Incorrect placement solution due to : {}",
					e.getMessage());
			}
		});
		return placementSolution;
	}

	public void updateState(
		double reward,
		int currentStateId,
		Function<Integer, Double> stateRewardFunction) {

		if (this.currentStateId != currentStateId) {
			log.info("Updating agent state with received reward : " + reward);

			this.previousStateId = this.currentStateId;
			this.currentStateId = currentStateId;

			Set<Integer> actionsAtState = stateSpaceMap.keySet();
			Transition currentTransition = new Transition(
				previousStateId,
				currentActionId,
				currentStateId,
				reward);
			agent.update(
				currentTransition.oldState,
				currentTransition.action,
				currentTransition.newState,
				actionsAtState,
				currentTransition.reward,
				stateRewardFunction);
			transitionList.add(currentTransition);
			if (isValidAction(currentTransition.action) && isValidState(currentTransition.oldState)
				&& isValidState(currentTransition.newState)) {
				flushToDB(currentTransition);
			}
		}
	}

	public int getSuggestedAction(int currentStateId) {
		Set<Integer> actionsAtState = stateSpaceMap.keySet();
		this.currentActionId = agent.selectAction(currentStateId, actionsAtState);
		return currentActionId;
	}

	private boolean isValidAction(int actionId) {
		return stateSpaceMap.containsKey(actionId);
	}

	private boolean isValidState(int stateId) {
		return stateSpaceMap.containsKey(stateId);
	}

	private void flushToDB(Transition transition) {
		List<Integer> actionAsList = stateSpaceMap.get(transition.action);
		List<Integer> oldStateAsList = stateSpaceMap.get(transition.oldState);
		List<Integer> newStateAsList = stateSpaceMap.get(transition.newState);

		if (actionAsList != null && oldStateAsList != null && newStateAsList != null) {
			StringBuilder actionStr = new StringBuilder();
			StringBuilder oldStateStr = new StringBuilder();
			StringBuilder newStateStr = new StringBuilder();
			for (int i = 0; i < nSchedulingSocketSlots; i++) {
				if (actionAsList.contains(i)) {
					actionStr.append(i).append(",");
				}
				if (oldStateAsList.contains(i)) {
					oldStateStr.append(1).append(",");
				} else {
					oldStateStr.append(0).append(",");
				}
				if (newStateAsList.contains(i)) {
					newStateStr.append(1).append(",");
				} else {
					newStateStr.append(0).append(",");
				}
			}
			actionStr.deleteCharAt(actionStr.length() - 1);
			oldStateStr.deleteCharAt(oldStateStr.length() - 1);
			newStateStr.deleteCharAt(newStateStr.length() - 1);

			influxDBTransitionsClient.writeQLearningActionToDB(
				actionStr.toString(),
				oldStateStr.toString(),
				newStateStr.toString(),
				transition.reward);
		}
	}

	public void shutdown() {
		if (influxDBTransitionsClient != null) {
			influxDBTransitionsClient.closeConnection();
		}
	}

}
