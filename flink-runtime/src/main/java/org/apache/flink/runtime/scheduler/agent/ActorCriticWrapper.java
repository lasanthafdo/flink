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

import com.github.chen0040.rl.learning.actorcritic.ActorCriticLearner;
import org.paukov.combinatorics.CombinatoricsVector;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.paukov.combinatorics.CombinatoricsFactory.createCompositionGenerator;
import static org.paukov.combinatorics.CombinatoricsFactory.createSimpleCombinationGenerator;

/**
 * Wrapper class for the actor-critic training.
 */
public class ActorCriticWrapper {

	private int stateCount;
	private int actionCount;
	private int previousStateId;

	private final ActorCriticLearner agent = new ActorCriticLearner(stateCount, actionCount);
	private final List<Transition> transitionList = new ArrayList<>();
	private InfluxDBTransitionsClient influxDBTransitionsClient;
	private final Map<Integer, List<Integer>> stateSpaceMap;
	private final int nCpus;
	private final Logger log;
	private int currentStateId;
	private int currentActionId;

	public ActorCriticWrapper(int nCpus, int nVertices, Logger log) {
		stateSpaceMap = generateActionSpace(nCpus, nVertices);
		this.stateCount = stateSpaceMap.size();
		this.actionCount = stateSpaceMap.size();
		this.nCpus = nCpus;
		this.log = log;

		setupInfluxDBConnection();
	}

	private Map<Integer, List<Integer>> generateActionSpace(int nCpus, int nVertices) {
		Map<Integer, List<Integer>> actionMap = new HashMap<>();
		CombinatoricsVector<Integer> cpuIds = new CombinatoricsVector<>();
		for (int i = 0; i < nCpus; i++) {
			cpuIds.addValue(i);
		}
		Generator<Integer> gen = createSimpleCombinationGenerator(cpuIds, nVertices);
		int actionId = 1;
		for (ICombinatoricsVector<Integer> cpuSelection : gen.generateAllObjects()) {
			actionMap.put(actionId++, cpuSelection.getVector());
		}
		return actionMap;
	}

	public int getStateFor(List<Integer> cpuAssignment) {
		return stateSpaceMap
			.entrySet()
			.stream()
			.filter(entry -> entry.getValue().size() == cpuAssignment.size() && entry
				.getValue()
				.containsAll(cpuAssignment))
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
			"flink-transitions", log);
		influxDBTransitionsClient.setup();
	}

	public List<Integer> getPlacementSolution(int action) {
		return stateSpaceMap.get(action);
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
			for (int i = 0; i < nCpus; i++) {
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

			influxDBTransitionsClient.writeToDB(
				actionStr.toString(),
				oldStateStr.toString(),
				newStateStr.toString(),
				transition.reward);
		}
	}

}
