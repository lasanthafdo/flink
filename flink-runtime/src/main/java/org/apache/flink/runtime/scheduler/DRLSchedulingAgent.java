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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionEdge;
import org.apache.flink.runtime.scheduler.adapter.SchedulingCpuCore;
import org.apache.flink.runtime.scheduler.adapter.SchedulingCpuSocket;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionEdge;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingRuntimeState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A scheduling agent that will run periodically to reschedule.
 */
public class DRLSchedulingAgent implements SchedulingAgent, SchedulingRuntimeState {

	public static final SchedulingCpuSocket schedulingCpuSocket = new SchedulingCpuSocket(new ArrayList<>(
		Arrays.asList(
			new SchedulingCpuCore(new ArrayList<>(Arrays.asList(0, 1))),
			new SchedulingCpuCore(new ArrayList<>(Arrays.asList(2, 3))),
			new SchedulingCpuCore(new ArrayList<>(Arrays.asList(4, 5))),
			new SchedulingCpuCore(new ArrayList<>(Arrays.asList(6, 7))),
			new SchedulingCpuCore(new ArrayList<>(Arrays.asList(8, 9))),
			new SchedulingCpuCore(new ArrayList<>(Arrays.asList(10, 11)))
		)));

	private final ExecutionGraph executionGraph;
	private final SchedulingStrategy schedulingStrategy;
	private final Logger log;
	private final long triggerPeriod;
	private final long waitTimeout;
	private final int numRetries;
	private final ActorCriticWrapper actorCriticWrapper = new ActorCriticWrapper(20, 10);

	private CompletableFuture<Collection<Acknowledge>> previousRescheduleFuture;

	private final SchedulingTopology schedulingTopology;
	private final Map<String, Double> edgeFlowRates;
	private final Map<String, SchedulingExecutionEdge<? extends SchedulingExecutionVertex, ? extends SchedulingResultPartition>> edgeMap;
	private final List<SchedulingExecutionVertex> sourceVertices = new ArrayList<>();
	private List<SchedulingExecutionEdge> orderedEdgeList;
	private InfluxDBMetricsClient influxDBMetricsClient;

	private int lastAction;

	public DRLSchedulingAgent(
		Logger log,
		ExecutionGraph executionGraph,
		SchedulingStrategy schedulingStrategy,
		long triggerPeriod,
		long waitTimeout,
		int numRetries) {

		this.log = log;
		this.executionGraph = checkNotNull(executionGraph);
		this.schedulingStrategy = checkNotNull(schedulingStrategy);
		this.triggerPeriod = triggerPeriod;
		this.waitTimeout = waitTimeout;
		this.numRetries = numRetries;

		this.edgeFlowRates = new HashMap<>();
		this.edgeMap = new HashMap<>();
		this.schedulingTopology = checkNotNull(executionGraph.getSchedulingTopology());

		initializeEdgeFlowRates();
		setupInfluxDBConnection();
	}

	@Override
	public long getTriggerPeriod() {
		return triggerPeriod;
	}

	private void initializeEdgeFlowRates() {
		schedulingTopology.getVertices().forEach(schedulingExecutionVertex -> {
			AtomicInteger consumerCount = new AtomicInteger(0);
			schedulingExecutionVertex.getConsumedResults().forEach(schedulingResultPartition -> {
				schedulingResultPartition
					.getConsumers()
					.forEach(consumer -> {
						DefaultExecutionEdge dee = new DefaultExecutionEdge(
							schedulingResultPartition.getProducer(),
							consumer,
							schedulingResultPartition);
						edgeMap.put(dee.getExecutionEdgeId(), dee);
					});
				consumerCount.getAndIncrement();
			});
			if (consumerCount.get() == 0) {
				sourceVertices.add(schedulingExecutionVertex);
			}
		});
	}

	private void setupInfluxDBConnection() {
		influxDBMetricsClient = new InfluxDBMetricsClient("http://127.0.0.1:8086", "flink");
		influxDBMetricsClient.setup();
	}

	private void updateState() {
		Map<String, Double> currentFlowRates = influxDBMetricsClient.getRateMetricsFor(
			"taskmanager_job_task_edge_numRecordsProcessedPerSecond",
			"edge_id",
			"rate");
		edgeFlowRates.putAll(currentFlowRates);
		orderedEdgeList = edgeFlowRates
			.entrySet()
			.stream()
			.sorted(Map.Entry.comparingByValue())
			.map(mapEntry -> edgeMap.get(mapEntry.getKey()))
			.collect(Collectors.toList());
	}

	@Override
	public void run() {
		updateState();
		if (previousRescheduleFuture == null || previousRescheduleFuture.isDone()) {
			log.info("Rescheduling job '" + executionGraph.getJobName() + "'");
			previousRescheduleFuture = rescheduleEager();
		}
	}

	private CompletableFuture<Collection<Acknowledge>> rescheduleEager() {
		checkState(executionGraph.getState() == JobStatus.RUNNING, "job is not running currently");

		final Iterable<ExecutionVertex> vertices = executionGraph.getAllExecutionVertices();
		final ArrayList<CompletableFuture<Acknowledge>> allHaltFutures = new ArrayList<>();

		for (ExecutionVertex ev : vertices) {
			Execution attempt = ev.getCurrentExecutionAttempt();
			CompletableFuture<Acknowledge> haltFuture = attempt
				.haltExecution()
				.whenCompleteAsync((ack, fail) -> {
					String taskNameWithSubtaskIndex = attempt
						.getVertex()
						.getTaskNameWithSubtaskIndex();
					for (int i = 0; i < numRetries; i++) {
						if (attempt.getState() != ExecutionState.CREATED) {
							try {
								Thread.sleep(waitTimeout);
							} catch (InterruptedException exception) {
								log.warn(
									"Thread waiting on halting of task {} was interrupted due to cause : {}",
									taskNameWithSubtaskIndex,
									exception);
							}
						} else {
							if (log.isDebugEnabled()) {
								log.debug("Task '" + taskNameWithSubtaskIndex
									+ "' changed to expected state (" +
									ExecutionState.CREATED + ") while waiting " + i + " times");
							}
							return;
						}
					}
					log.error("Couldn't halt execution for task {}.", taskNameWithSubtaskIndex);
					FutureUtils.completedExceptionally(new Exception(
						"Couldn't halt execution for task " + taskNameWithSubtaskIndex));
				});
			allHaltFutures.add(haltFuture);
		}
		final FutureUtils.ConjunctFuture<Collection<Acknowledge>> allHaltsFuture = FutureUtils.combineAll(
			allHaltFutures);
		return allHaltsFuture.whenComplete((ack, fail) -> {
			if (fail != null) {
				log.error("Encountered exception when halting process.", fail);
				throw new CompletionException("Halt process unsuccessful", fail);
			} else {
				schedulingStrategy.startScheduling();
			}
		});
	}

	@Override
	public List<SchedulingExecutionVertex> getSourceVertices() {
		return sourceVertices;
	}

	@Override
	public Map<String, Double> getEdgeThroughput() {
		return edgeFlowRates;
	}

	@Override
	public List<SchedulingExecutionEdge> getOrderedEdgeList() {
		return orderedEdgeList;
	}

	@Override
	public double getOverallThroughput() {
		return 0;
	}

}
