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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionContainer;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A scheduling agent that will run periodically to reschedule.
 */
public class QActorCriticSchedulingAgent extends AbstractSchedulingAgent {

	private QActorCriticModel qActorCriticModel;
	private CompletableFuture<Collection<Acknowledge>> previousRescheduleFuture;
	private final ScheduledExecutorService executorService;
	private final long updatePeriodInSeconds;

	public QActorCriticSchedulingAgent(
		Logger log,
		ExecutionGraph executionGraph,
		SchedulingStrategy schedulingStrategy,
		ScheduledExecutorService executorService,
		long triggerPeriod,
		long waitTimeout,
		int numRetries,
		int updatePeriod) {

		super(
			log,
			triggerPeriod,
			executionGraph,
			schedulingStrategy,
			waitTimeout,
			numRetries, 4);

		this.executorService = checkNotNull(executorService);
		this.updatePeriodInSeconds = updatePeriod;
		setupUpdateTriggerThread();
	}

	private void setupUpdateTriggerThread() {
		updateExecutor = executorService.scheduleAtFixedRate(
			this::executeUpdateProcess,
			updatePeriodInSeconds,
			updatePeriodInSeconds,
			TimeUnit.SECONDS);
	}

	private void executeUpdateProcess() {
		try {
			updateStateInformation();
			updateCurrentPlacementInformation();
			updatePlacementSolution();
		} catch (Exception e) {
			log.error(
				"Encountered exception when trying to update state: {}",
				e.getMessage(),
				e);
		}
	}

	@Override
	protected void onResourceInitialization() {
		List<Tuple2<InetAddress, Integer>> nodeSocketCounts = new ArrayList<>();
		List<InetAddress> nodes = taskManLocSlotCountMap
			.values()
			.stream()
			.map(taskManLoc -> taskManLoc.f0.address())
			.distinct()
			.collect(Collectors.toList());
		nodes.forEach(ipAddress -> nodeSocketCounts.add(new Tuple2<>(
			ipAddress,
			cpuLayout.sockets())));
		this.qActorCriticModel = new QActorCriticModel(
			nodeSocketCounts,
			this.nVertices,
			cpuLayout.coresPerSocket() * cpuLayout.threadsPerCore(),
			log);
	}

	@Override
	protected void updatePlacementSolution() {
		int currentStateId = qActorCriticModel.getStateFor(currentPlacementAction);
		qActorCriticModel.updateState(
			getOverallThroughput(),
			currentStateId,
			stateId -> -1.0
				* getTopLevelContainer().getResourceUsage(SchedulingExecutionContainer.CPU));
		int currentAction = qActorCriticModel.getSuggestedAction(currentStateId);
		List<Tuple2<InetAddress, Integer>> modelSuggestedPlacementAction = qActorCriticModel.getPlacementSolution(
			currentAction);
		setFromModelPlacementAction(modelSuggestedPlacementAction);
		if (!isValidPlacementAction(suggestedPlacementAction)) {
			throw new FlinkRuntimeException(
				"Invalid placement action " + suggestedPlacementAction + " suggested.");
		}
	}

	private void setFromModelPlacementAction(List<Tuple2<InetAddress, Integer>> modelSuggestedPlacementAction) {
		suggestedPlacementAction.clear();
		List<Tuple2<TaskManagerLocation, Integer>> taskManSlots = new ArrayList<>(nSchedulingSlots);
		taskManLocSlotCountMap.values().forEach(taskManSlotCountElem -> {
			for (int i = 0; i < taskManSlotCountElem.f1; i++) {
				taskManSlots.add(new Tuple2<>(taskManSlotCountElem.f0, i));
			}
		});
		modelSuggestedPlacementAction.forEach(operatorPlacement -> {
			Tuple2<TaskManagerLocation, Integer> suitableTaskMan = taskManSlots
				.stream()
				.filter(taskManLocSlotCountElem -> taskManLocSlotCountElem.f0
					.address()
					.equals(operatorPlacement.f0)).findFirst().orElse(new Tuple2<>(null, -1));
			suggestedPlacementAction.add(new Tuple3<>(
				suitableTaskMan.f0,
				-1,
				operatorPlacement.f1));
		});
	}

	@Override
	public void run() {
		if (previousRescheduleFuture == null || previousRescheduleFuture.isDone()) {
			executeUpdateProcess();
			if (suggestedPlacementAction.equals(currentPlacementAction)) {
				log.info(
					"Current placement action {} is the same as the suggested placement action {}. Skipping rescheduling.",
					currentPlacementAction,
					suggestedPlacementAction);
			} else {
				log.info("Rescheduling job '" + executionGraph.getJobName() + "'");
				previousRescheduleFuture = rescheduleEager();
			}
		}
	}

}
