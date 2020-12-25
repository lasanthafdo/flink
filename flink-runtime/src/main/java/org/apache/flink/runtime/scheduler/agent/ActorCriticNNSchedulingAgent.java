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

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A scheduling agent that will run periodically to reschedule.
 */
public class ActorCriticNNSchedulingAgent extends AbstractSchedulingAgent {

	private final NeuralNetworksBasedActorCriticModel actorCriticModel;

	private CompletableFuture<Collection<Acknowledge>> previousRescheduleFuture;

	private final Map<List<Integer>, Double> potentialPlacementActions;
	private final ScheduledExecutorService actorCriticExecutor;
	private final int updatePeriodInSeconds;
	private final long trainingPhaseUpdatePeriodInSeconds;
	private int trainingPhaseUpdateCount = 0;
	private boolean inTrainingPhase = true;
	private final int frequentUpdatesThreshold;

	public ActorCriticNNSchedulingAgent(
		Logger log,
		ExecutionGraph executionGraph,
		SchedulingStrategy schedulingStrategy,
		ScheduledExecutorService actorCriticExecutor,
		long triggerPeriod,
		long waitTimeout,
		int numRetries,
		int updatePeriodInSeconds,
		int tpUpdatePeriod,
		int freqUpdateThreshold,
		NeuralNetworkConfiguration neuralNetworkConfiguration) {

		super(log, triggerPeriod, executionGraph, schedulingStrategy, waitTimeout, numRetries);

		int nCpus = cpuLayout.cpus();
		int numInputs = (nVertices + 1) * nCpus + edgeMap.size();
		neuralNetworkConfiguration.setNumInputs(numInputs);
		this.actorCriticModel = new NeuralNetworksBasedActorCriticModel(
			nCpus, nVertices, neuralNetworkConfiguration, log);
		this.actorCriticExecutor = actorCriticExecutor;
		this.updatePeriodInSeconds = updatePeriodInSeconds;
		this.potentialPlacementActions = new TopKMap<>(neuralNetworkConfiguration.getNumActionSuggestions());
		this.trainingPhaseUpdatePeriodInSeconds = tpUpdatePeriod;
		this.frequentUpdatesThreshold = freqUpdateThreshold;
		setupUpdateTriggerThread();
	}

	private void setupUpdateTriggerThread() {
		if (inTrainingPhase) {
			updateExecutor = actorCriticExecutor.scheduleAtFixedRate(() -> {
					executeUpdateProcess();
					rescheduleEager();
					if (trainingPhaseUpdateCount++ >= frequentUpdatesThreshold) {
						inTrainingPhase = false;
						log.info(
							"Leaving training phase. Update count is {}",
							trainingPhaseUpdateCount);
						setupUpdateTriggerThread();
					}
				},
				trainingPhaseUpdatePeriodInSeconds, trainingPhaseUpdatePeriodInSeconds,
				TimeUnit.SECONDS);
		} else {
			if (updateExecutor != null) {
				updateExecutor.cancel(false);
			}
			updateExecutor = actorCriticExecutor.scheduleAtFixedRate(
				this::executeUpdateProcess,
				updatePeriodInSeconds,
				updatePeriodInSeconds,
				TimeUnit.SECONDS);
		}
	}

	private void executeUpdateProcess() {
		try {
			updateStateInformation();
			updateCurrentPlacementActionInformation();
			if (!currentPlacementAction.isEmpty()) {
				actorCriticModel.updateTrainingData(
					currentPlacementAction,
					new ArrayList<>(getCpuMetrics().values()),
					new ArrayList<>(getEdgeThroughput().values()),
					getOverallThroughput());
			}
			updatePlacementSolution();
		} catch (Exception e) {
			log.error(
				"Encountered exception when trying to update training data: {}",
				e.getMessage(),
				e);
		}
	}

	@Override
	public List<Integer> getPlacementSolution() {
		return suggestedPlacementAction;
	}

	@Override
	protected void updatePlacementSolution() {
		double currentThroughput = getOverallThroughput();
		potentialPlacementActions.put(currentPlacementAction, currentThroughput);
		potentialPlacementActions.put(getTrafficBasedPlacementAction(), currentThroughput);

		suggestedPlacementAction = actorCriticModel.selectAction(
			potentialPlacementActions,
			currentThroughput,
			new ArrayList<>(getCpuMetrics().values()),
			new ArrayList<>(edgeFlowRates.values()));
		if (suggestedPlacementAction == null || suggestedPlacementAction.isEmpty()) {
			suggestedPlacementAction = getTrafficBasedPlacementAction();
		}
		if (!isValidPlacementAction(suggestedPlacementAction)) {
			throw new FlinkRuntimeException(
				"Invalid placement action " + suggestedPlacementAction + " suggested.");
		}
		log.info(
			"Suggested placement action: {}, current placement action: {}, throughput: {}",
			suggestedPlacementAction,
			currentPlacementAction,
			currentThroughput);
	}

	@Override
	public void run() {
		if (!inTrainingPhase && (previousRescheduleFuture == null
			|| previousRescheduleFuture.isDone())) {
			executeUpdateProcess();
			try {
				if (suggestedPlacementAction.equals(currentPlacementAction)) {
					log.info(
						"Current placement action {} is the same as the suggested placement action {}. Skipping rescheduling.",
						currentPlacementAction,
						suggestedPlacementAction);
				} else {
					log.info("Rescheduling job '" + executionGraph.getJobName() + "'");
					previousRescheduleFuture = rescheduleEager();
				}
			} catch (Exception e) {
				log.error(
					"Encountered exception while trying to reschedule : {}",
					e.getMessage(),
					e);
			}
		}
	}

}
