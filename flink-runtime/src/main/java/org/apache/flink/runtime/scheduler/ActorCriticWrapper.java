package org.apache.flink.runtime.scheduler;

import com.github.chen0040.rl.learning.actorcritic.ActorCriticLearner;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
	private InfluxDBMetricsClient influxDBMetricsClient;
	private Logger log;

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

	public ActorCriticWrapper(int actionCount, int stateCount, Logger log) {
		this.stateCount = stateCount;
		this.actionCount = actionCount;
		this.log = log;
		setupInfluxDBConnection();
	}

	private void setupInfluxDBConnection() {
		influxDBMetricsClient = new InfluxDBMetricsClient(
			"http://127.0.0.1:8086",
			"flink-transitions", log);
		influxDBMetricsClient.setup();
	}

	public int getSuggestedAction(double previousReward, int currentStateId) {
		System.out.println("Agent does action-" + previousActionId);
		System.out.println("Agent receives Reward = " + previousReward);

		transitionList.add(new Transition(
			previousStateId,
			previousActionId,
			currentStateId,
			previousReward));
		int actionId = agent.selectAction(currentStateId);
		previousStateId = currentStateId;
		previousActionId = actionId;
		return actionId;
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

	public void flushTransitionsToDB() {

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
