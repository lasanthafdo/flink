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
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A scheduling agent that will run periodically to reschedule.
 */
public class QActorCriticSchedulingAgent extends AbstractSchedulingAgent {

	private final QActorCriticWrapper qActorCriticWrapper;
	private CompletableFuture<Collection<Acknowledge>> previousRescheduleFuture;
	private final ScheduledExecutorService executorService;
	private final long updatePeriodInSeconds;

	public QActorCriticSchedulingAgent(
		Logger log,
		ExecutionGraph executionGraph,
		SchedulingStrategy schedulingStrategy,
		SlotPool slotPool,
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

		int nVertices = Math.toIntExact(StreamSupport.stream(executionGraph
			.getSchedulingTopology()
			.getVertices().spliterator(), false).count());
		this.qActorCriticWrapper = new QActorCriticWrapper(this.nCpus, nVertices, log);
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
	protected void updatePlacementSolution() {
/*
		Tuple2<TaskManagerLocation, Integer> currentStateId = qActorCriticWrapper.getStateFor(currentPlacementAction);
		qActorCriticWrapper.updateState(
			getOverallThroughput(),
			currentStateId,
			stateId -> -1.0
				* getTopLevelContainer().getResourceUsage(SchedulingExecutionContainer.CPU));
		int currentAction = qActorCriticWrapper.getSuggestedAction(currentStateId);
		suggestedPlacementAction = qActorCriticWrapper.getPlacementSolution(currentAction);
		if (!isValidPlacementAction(suggestedPlacementAction)) {
			throw new FlinkRuntimeException(
				"Invalid placement action " + suggestedPlacementAction + " suggested.");
		}
*/
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
