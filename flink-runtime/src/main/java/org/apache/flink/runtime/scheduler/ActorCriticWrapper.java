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

package org.apache.flink.runtime.scheduler;

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
	private int previousActionId;

	private final ActorCriticLearner agent = new ActorCriticLearner(stateCount, actionCount);
	private final List<Transition> transitionList = new ArrayList<>();
	private InfluxDBTransitionsClient influxDBTransitionsClient;
	private final Map<Integer, List<Integer>> stateSpaceMap;
	private final int nCpus;
	private final Logger log;

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

	private Map<Integer, List<Integer>> generateStateSpace(
		int nVertices,
		int maxPerCore,
		int maxCores) {
		long start = System.currentTimeMillis();
		Generator<Integer> gen = createCompositionGenerator(nVertices);

		List<ICombinatoricsVector<Integer>> filteredList = gen.generateFilteredObjects((index, integers) ->
			integers.getSize() <= maxCores
				&& integers.getVector().stream().max(Comparator.naturalOrder()).orElse(0)
				<= maxPerCore);
		int count = 0;
		Map<Integer, List<Integer>> stateMap = new HashMap<>();
		for (ICombinatoricsVector<Integer> p : filteredList) {
			count++;
			stateMap.put(count, p.getVector());
		}
		log.info(
			"Added {} actions for {} vertices with maximum limit of {} in {} seconds.",
			count,
			nVertices,
			maxPerCore,
			(System.currentTimeMillis() - start));
		return stateMap;
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

	public int getSuggestedAction(
		double previousReward,
		int currentStateId, Function<Integer, Double> stateRewardFunction) {
		log.info("Agent does action : " + previousActionId);
		log.info("Agent receives reward : " + previousReward);

		Set<Integer> actionsAtState = stateSpaceMap.keySet();
		Transition previousTransition = new Transition(
			previousStateId,
			previousActionId,
			currentStateId,
			previousReward);
		agent.update(
			previousTransition.oldState,
			previousTransition.action,
			previousTransition.newState,
			actionsAtState,
			previousTransition.reward,
			stateRewardFunction);
		int actionId = agent.selectAction(currentStateId, actionsAtState);
		previousStateId = currentStateId;
		previousActionId = actionId;
		transitionList.add(previousTransition);
		if (isValidAction(previousTransition.action) && isValidState(previousTransition.oldState)
			&& isValidState(previousTransition.newState)) {
			flushToDB(previousTransition);
		}
		return actionId;
	}

	private boolean isValidAction(int actionId) {
		return stateSpaceMap.containsKey(actionId);
	}

	private boolean isValidState(int stateId) {
		return stateSpaceMap.containsKey(stateId);
	}

	public void updateModel(final List<Transition> transitionSamples) {
		for (int i = transitionSamples.size() - 1; i >= 0; --i) {
			Transition nextTransition = transitionSamples.get(i);
			if (i != transitionSamples.size() - 1) {
				nextTransition = transitionSamples.get(i + 1);
			}
			Transition currentTransition = transitionSamples.get(i);
			agent.update(
				currentTransition.oldState,
				currentTransition.action,
				currentTransition.newState,
				Collections.singleton(nextTransition.action),
				currentTransition.reward,
				(stateId) -> 0d);
		}
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

	public void initMethod() {
/*
		ActorCriticAgent agent = new ActorCriticAgent(stateCount, actionCount);
		Vec stateValues = new Vec(stateCount);

		Random random = new Random();
		agent.start(random.nextInt(stateCount));
		for (
			int time = 0;
			time < 1000; ++time) {

			int actionId = agent.selectAction();
			System.out.println("Agent does action-" + actionId);

			int newStateId = world.update(agent, actionId);
			double reward = world.reward(agent);

			System.out.println("Now the new state is " + newStateId);
			System.out.println("Agent receives Reward = " + reward);

			//TODO set CPU usage, edge flow rates
			System.out.println("World state values changed ...");
			for (int stateId = 0; stateId < stateCount; ++stateId) {
				// Set the state values for each state
				stateValues.set(stateId, random.nextDouble());
			}

			agent.update(actionId, newStateId, reward, stateValues);
		}
*/
	}

}
