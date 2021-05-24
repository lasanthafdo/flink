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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.apache.flink.shaded.guava18.com.google.common.collect.BiMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.HashBiMap;

import com.github.chen0040.rl.learning.actorcritic.ActorCriticLearner;
import com.github.chen0040.rl.models.QModel;
import org.paukov.combinatorics3.Generator;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Wrapper class for the actor-critic training.
 */
public class QActorCriticModel {

	private static final String INET_ADDR_SOCKET_DELIM = ":";
	private static final String RETENTION_POLICY_NAME = "one_day";
	public static final int DEFAULT_NUM_STATES_INCREMENTED = 5;
	private final Map<String, Tuple2<TaskManagerLocation, Integer>> taskManLocSlotCountMap;

	private int topLevelStateCount;
	private int topLevelActionCount;
	private int previousTopLevelStateId;
	private int previousNodeLevelStateId;
	private int currentTopLevelStateId;
	private int currentNodeLevelStateId;
	private int currentTopLevelActionId;
	private int currentNodeLevelActionId;

	private final ActorCriticLearner topLevelAgent = new ActorCriticLearner(
		topLevelStateCount,
		topLevelActionCount);
	private final ActorCriticLearner nodeLevelAgent = new ActorCriticLearner(1, 1);
	private final List<Transition> transitionList = new ArrayList<>();
	private InfluxDBTransitionsClient influxDBTransitionsClient;
	private final BiMap<Integer, Map<Integer, Long>> topLevelStateSpaceMap;
	private final BiMap<Integer, List<Integer>> nodeLevelStateSpaceMap;
	private final BiMap<String, Integer> sockAddrToSockIdMap;
	private final int nSchedulingSocketSlots;
	private final int nVertices;
	private final int nProcUnitsPerSocket;
	private final BiMap<Tuple3<ExecutionVertexID, String, Integer>, Integer> orderedOperatorMap;
	private final Logger log;

	public QActorCriticModel(
		List<Tuple2<InetAddress, Integer>> nodeSocketCounts,
		int nVertices,
		int nProcessingUnitsPerSocket,
		Map<Tuple3<ExecutionVertexID, String, Integer>, Integer> orderedOperatorMap,
		Map<String, Tuple2<TaskManagerLocation, Integer>> taskManLocSlotCountMap,
		Logger log) {
		this.sockAddrToSockIdMap = HashBiMap.create();
		this.nSchedulingSocketSlots = getAvailableSlotsAfterDerivation(nodeSocketCounts);
		this.nVertices = nVertices;
		this.nProcUnitsPerSocket = nProcessingUnitsPerSocket;
		this.orderedOperatorMap = HashBiMap.create(orderedOperatorMap);
		this.topLevelStateSpaceMap = generateTopLevelActionSpace();
		this.nodeLevelStateSpaceMap = HashBiMap.create();
		int topLevelStateActionCount = topLevelStateSpaceMap.size();
		this.topLevelStateCount = topLevelStateActionCount;
		this.topLevelActionCount = topLevelStateActionCount;
		this.log = log;
		this.taskManLocSlotCountMap = taskManLocSlotCountMap;

		setupInfluxDBConnection();
	}

	@VisibleForTesting
	QActorCriticModel(
		List<Tuple2<InetAddress, Integer>> nodeSocketCounts,
		int nVertices,
		int nProcessingUnitsPerSocket,
		Map<Tuple3<ExecutionVertexID, String, Integer>, Integer> orderedOperatorMap) {
		this.sockAddrToSockIdMap = HashBiMap.create();
		this.nSchedulingSocketSlots = getAvailableSlotsAfterDerivation(nodeSocketCounts);
		this.nVertices = nVertices;
		this.nProcUnitsPerSocket = nProcessingUnitsPerSocket;
		this.orderedOperatorMap = HashBiMap.create(orderedOperatorMap);
		this.topLevelStateSpaceMap = generateTopLevelActionSpace();
		this.nodeLevelStateSpaceMap = HashBiMap.create();
		this.topLevelStateCount = topLevelStateSpaceMap.size();
		this.topLevelActionCount = topLevelStateSpaceMap.size();
		this.taskManLocSlotCountMap = null;
		this.log = null;
	}

	private String getSocketAddress(InetAddress node, Integer socket) {
		return node.getHostAddress() + INET_ADDR_SOCKET_DELIM + socket;
	}

	private String getNodeIpAddress(String socketAddress) {
		String[] socketIdParts = socketAddress.split(INET_ADDR_SOCKET_DELIM);
		return socketIdParts[0];
	}

	private Tuple2<InetAddress, Integer> getSocketId(String socketAddress) throws UnknownHostException {
		String[] socketIdParts = socketAddress.split(INET_ADDR_SOCKET_DELIM);
		return new Tuple2<>(
			InetAddress.getByName(socketIdParts[0]),
			Integer.valueOf(socketIdParts[1]));
	}

	private int getAvailableSlotsAfterDerivation(List<Tuple2<InetAddress, Integer>> nodeSocketCounts) {
		AtomicInteger scheduleIdCount = new AtomicInteger(0);
		nodeSocketCounts.forEach(locSlotCount -> {
			for (int i = 0; i < locSlotCount.f1; i++) {
				sockAddrToSockIdMap.put(
					getSocketAddress(locSlotCount.f0, i),
					scheduleIdCount.incrementAndGet());
			}
		});
		return scheduleIdCount.get();
	}

	@VisibleForTesting
	void addToSocketScheduleIdMap(String socketAddress) {
		Integer currentMaxId = sockAddrToSockIdMap
			.values()
			.stream()
			.max(Comparator.naturalOrder())
			.orElse(0);
		sockAddrToSockIdMap.put(socketAddress, currentMaxId + 1);
	}

	@VisibleForTesting
	Map<Integer, List<Integer>> generateStateActionSpace(int nVertices, int nProcUnitsPerSocket) {
		Map<Integer, List<Integer>> actionMap = new HashMap<>();
		Set<List<Integer>> actionSet = new HashSet<>();
		List<Integer> socketIds = new ArrayList<>();
		sockAddrToSockIdMap.values().forEach(socketId -> {
			for (int i = 0; i < nProcUnitsPerSocket; i++) {
				socketIds.add(socketId);
			}
		});
		AtomicInteger actionId = new AtomicInteger(1);
		Generator
			.combination(socketIds)
			.simple(nVertices)
			.stream()
			.forEach(currentCombo -> Generator
				.permutation(currentCombo)
				.simple()
				.forEach(actionSet::add));

		actionSet.forEach(action -> actionMap.put(actionId.getAndIncrement(), action));
		return actionMap;
	}

	@VisibleForTesting
	BiMap<Integer, Map<Integer, Long>> generateTopLevelActionSpace() {
		BiMap<Integer, Map<Integer, Long>> actionMap = HashBiMap.create();
		List<List<Integer>> combinationList = generateCombinationsFor(
			nVertices,
			nProcUnitsPerSocket);
		AtomicInteger actionId = new AtomicInteger(1);
		combinationList.forEach(combination -> {
			Map<Integer, Long> socketIdOpCount = combination
				.stream()
				.collect(Collectors.groupingBy(socketId -> socketId, Collectors.counting()));
			actionMap.put(actionId.getAndIncrement(), socketIdOpCount);
		});
		return actionMap;
	}

	@VisibleForTesting
	List<List<Integer>> generateCombinationsFor(int nVertices, int nProcUnitsPerSocket) {
		Map<Integer, List<List<Integer>>> preCombinationMap = new HashMap<>();
		List<List<Integer>> combinationList = new ArrayList<>();
		sockAddrToSockIdMap.values().forEach(scheduleId -> {
			List<List<Integer>> preCombinationList = new ArrayList<>();
			for (int i = 0; i < nProcUnitsPerSocket; i++) {
				List<Integer> socketContributionList = new ArrayList<>();
				for (int j = 0; j <= i; j++) {
					socketContributionList.add(scheduleId);
				}
				preCombinationList.add(socketContributionList);
			}
			preCombinationMap.put(scheduleId, preCombinationList);
		});
		getPossibleLists(
			nVertices,
			preCombinationMap,
			sockAddrToSockIdMap.size(),
			combinationList,
			new ArrayList<>());
		return combinationList;
	}

	private void getPossibleLists(
		int nVertices,
		Map<Integer, List<List<Integer>>> preCombinationMap,
		int socketId,
		List<List<Integer>> resultList,
		List<Integer> currentCombination) {
		if (nVertices <= 0 || socketId <= 0) {
			if (nVertices == 0 && socketId == 0) {
				resultList.add(new ArrayList<>(currentCombination));
			}
			return;
		}

		preCombinationMap.get(socketId).forEach(preCombination -> {
			int remainingVertices = nVertices - preCombination.size();
			Map<Integer, List<List<Integer>>> remainingPreCombinations = preCombinationMap
				.entrySet()
				.stream()
				.filter(entrySet -> entrySet.getKey() != socketId)
				.collect(
					Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
			List<Integer> nextCombination = new ArrayList<>(currentCombination);
			nextCombination.addAll(preCombination);
			getPossibleLists(
				remainingVertices,
				remainingPreCombinations,
				socketId - 1,
				resultList,
				nextCombination);
		});
	}

	void generateActionFor(
		Map<Integer, Long> selectedCombination) {
		List<Integer> currentAction = new ArrayList<>(nVertices);
		selectedCombination.forEach((socketId, opCount) -> {
			for (int i = 0; i < opCount; i++) {
				currentAction.add(socketId);
			}
		});
		int i = 0;
		int j = 0;
		while (i < DEFAULT_NUM_STATES_INCREMENTED && j < DEFAULT_NUM_STATES_INCREMENTED * 2) {
			j++;
			Collections.shuffle(currentAction);
			if (isValidOperatorPlacement(currentAction) && insertStateOrAction(currentAction)) {
				i++;
			}
		}
	}

	private boolean insertStateOrAction(List<Integer> currentStateOrAction) {
		boolean insertSuccess = false;
		if (!nodeLevelStateSpaceMap.containsValue(currentStateOrAction)) {
			int nextKey = nodeLevelStateSpaceMap
				.keySet()
				.stream()
				.mapToInt(actionId -> actionId)
				.max()
				.orElse(0) + 1;
			nodeLevelStateSpaceMap.put(nextKey, new ArrayList<>(currentStateOrAction));
			int stateOrActionCount = nodeLevelStateSpaceMap.size();
			QModel nodeLevelQModel = nodeLevelAgent.getP();
			nodeLevelQModel.setStateCount(stateOrActionCount);
			nodeLevelQModel.setActionCount(stateOrActionCount);
			insertSuccess = true;
		}
		return insertSuccess;
	}

	public Tuple2<Integer, Integer> getStateFor(List<Tuple4<TaskManagerLocation, SlotSharingGroup, Integer, Integer>> cpuAssignment) {
		List<Integer> cpuAssignmentStateVector = cpuAssignment
			.stream()
			.map(operatorLoc -> sockAddrToSockIdMap.get(getSocketAddress(
				operatorLoc.f0.address(),
				operatorLoc.f3)))
			.collect(Collectors.toList());
		Map<Integer, Long> sockIdOpCountMap = cpuAssignmentStateVector.stream()
			.collect(Collectors.groupingBy(socketId -> socketId, Collectors.counting()));
		int topLevelStateId = topLevelStateSpaceMap.inverse().get(sockIdOpCountMap);
		// Insert should only succeed if the state is not already there
		if (insertStateOrAction(cpuAssignmentStateVector)) {
			log.info(
				"New state {} inserted into node-level state space map",
				cpuAssignmentStateVector);
		}
		int nodeLevelStateId = nodeLevelStateSpaceMap.inverse().get(cpuAssignmentStateVector);
		return new Tuple2<>(topLevelStateId, nodeLevelStateId);
	}

	static class Transition {
		int oldTopLevelState;
		int newTopLevelState;
		int topLevelAction;
		int oldNodeLevelState;
		int newNodeLevelState;
		int nodeLevelAction;
		double reward;

		public Transition(
			int oldTopLevelState,
			int topLevelAction,
			int newTopLevelState,
			int oldNodeLevelState,
			int nodeLevelAction,
			int newNodeLevelState,
			double reward) {

			this.oldTopLevelState = oldTopLevelState;
			this.newTopLevelState = newTopLevelState;
			this.topLevelAction = topLevelAction;
			this.oldNodeLevelState = oldNodeLevelState;
			this.newNodeLevelState = newNodeLevelState;
			this.nodeLevelAction = nodeLevelAction;
			this.reward = reward;
		}
	}

	private void setupInfluxDBConnection() {
		influxDBTransitionsClient = new InfluxDBTransitionsClient(
			"http://127.0.0.1:8086",
			"flink-transitions", RETENTION_POLICY_NAME, log);
		influxDBTransitionsClient.setup();
	}

	public List<Tuple2<InetAddress, Integer>> getPlacementSolution(
		int nodeLevelAction) {
		List<Tuple2<InetAddress, Integer>> placementSolution = new ArrayList<>();
		List<Integer> placementAction = nodeLevelStateSpaceMap.get(nodeLevelAction);
		placementAction.forEach(stateId -> {
			try {
				placementSolution.add(getSocketId(sockAddrToSockIdMap
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
		Tuple2<Integer, Integer> currentStateId,
		Function<Integer, Double> stateRewardFunction) {

		if (this.currentTopLevelStateId != currentStateId.f0
			|| this.currentNodeLevelStateId != currentStateId.f1) {
			log.info("Updating agent state with received reward : " + reward);

			this.previousTopLevelStateId = this.currentTopLevelStateId;
			this.previousNodeLevelStateId = this.currentNodeLevelStateId;
			this.currentTopLevelStateId = currentStateId.f0;
			this.currentNodeLevelStateId = currentStateId.f1;

			Set<Integer> topLevelActionsAtState = topLevelStateSpaceMap.keySet();
			Set<Integer> nodeLevelActionsAtState = nodeLevelStateSpaceMap.keySet();
			Transition currentTransition = new Transition(
				previousTopLevelStateId,
				currentTopLevelActionId,
				currentStateId.f0,
				previousNodeLevelStateId,
				currentNodeLevelActionId,
				currentStateId.f1,
				reward);
			topLevelAgent.update(
				currentTransition.oldTopLevelState,
				currentTransition.topLevelAction,
				currentTransition.newTopLevelState,
				topLevelActionsAtState,
				currentTransition.reward,
				stateRewardFunction);
			nodeLevelAgent.update(
				currentTransition.oldNodeLevelState,
				currentTransition.nodeLevelAction,
				currentTransition.newNodeLevelState,
				nodeLevelActionsAtState,
				currentTransition.reward,
				stateRewardFunction
			);
			transitionList.add(currentTransition);
			if (isValidAction(currentTransition.topLevelAction)
				&& isValidState(currentTransition.oldTopLevelState)
				&& isValidState(currentTransition.newTopLevelState)) {
				flushToDB(currentTransition);
			}
		}
	}

	public Tuple2<Integer, Integer> getSuggestedAction(
		int currentTopLevelStateId,
		int currentNodeLevelStateId) {

		Set<Integer> topLevelActionsAtState = topLevelStateSpaceMap.keySet();
		this.currentTopLevelActionId = topLevelAgent.selectAction(
			currentTopLevelStateId,
			topLevelActionsAtState);
		generateActionFor(topLevelStateSpaceMap.get(currentTopLevelActionId));
		Set<Integer> nodeLevelActionsAtState = nodeLevelStateSpaceMap.keySet();
		this.currentNodeLevelActionId = nodeLevelAgent.selectAction(
			currentNodeLevelStateId,
			nodeLevelActionsAtState);
		return new Tuple2<>(this.currentTopLevelActionId, this.currentNodeLevelActionId);
	}

	private boolean isValidAction(int actionId) {
		return topLevelStateSpaceMap.containsKey(actionId);
	}

	private boolean isValidOperatorPlacement(List<Integer> operatorPlacement) {
		boolean validSize = operatorPlacement.size() == orderedOperatorMap.size();
		List<Tuple2<String, String>> operatorHostPlacement = new ArrayList<>();
		Map<Integer, String> sockIdToSockAddr = sockAddrToSockIdMap.inverse();
		Map<Integer, Tuple3<ExecutionVertexID, String, Integer>> inverseOrderedOperatorMap = orderedOperatorMap
			.inverse();
		for (int i = 0; i < operatorPlacement.size(); i++) {
			operatorHostPlacement.add(new Tuple2<>(
				getNodeIpAddress(sockIdToSockAddr.get(operatorPlacement.get(i))),
				inverseOrderedOperatorMap.get(i).f1));
		}
		int opPlacementMaxNodeLevelParallelism = operatorHostPlacement
			.stream()
			.collect(Collectors.groupingBy(
				opNodeAssignment -> opNodeAssignment,
				Collectors.counting()))
			.values()
			.stream()
			.mapToInt(Long::intValue).max().orElse(0);
		int maxParallelism = taskManLocSlotCountMap
			.values()
			.stream()
			.mapToInt(tuple -> tuple.f1)
			.max()
			.orElse(0);
		boolean validParallelism = opPlacementMaxNodeLevelParallelism <= maxParallelism;
		if (!validParallelism) {
			log.warn("Invalid parallelism for node level operator placement {}", operatorPlacement);
		}
		return validSize && validParallelism;
	}

	private boolean isValidState(int stateId) {
		return topLevelStateSpaceMap.containsKey(stateId);
	}

	private void flushToDB(Transition transition) {
		List<Integer> actionAsList = nodeLevelStateSpaceMap.get(transition.nodeLevelAction);
		List<Integer> oldStateAsList = nodeLevelStateSpaceMap.get(transition.oldNodeLevelState);
		List<Integer> newStateAsList = nodeLevelStateSpaceMap.get(transition.newNodeLevelState);

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

			influxDBTransitionsClient.writeQacActionToDB(
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
