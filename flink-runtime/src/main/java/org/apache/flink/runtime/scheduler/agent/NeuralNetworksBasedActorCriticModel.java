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

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Wrapper class for the actor-critic training.
 */
public class NeuralNetworksBasedActorCriticModel {

	public static final int PLACEMENT_TYPE_PREDICTED = 1;
	public static final double DIMINISHING_STEP_SIZE = 0.01;
	public static final int PLACEMENT_TYPE_RANDOM = 2;
	public static final int PLACEMENT_TYPE_NAIVE = 3;
	private final int nVertices;
	private final int nCpus;

	private final Logger log;
	private final int numHiddenNodes;
	private final int maxTrainingCacheSize;
	private final int scoreIterationPrintFrequency;
	private MultiLayerNetwork net;
	private final long seed;
	private final int numInputs;
	private final int numOutputs;
	private final int nEpochs;
	private final double learningRate;
	private final int trainTriggerThreshold;
	private final double epsilonGreedyThreshold;
	private double diminishingGreedyThreshold;
	private final Random rand;

	private MultiLayerConfiguration conf;
	private Cache<INDArray, INDArray> trainingCache;
	private boolean isTrained = false;
	private int updatesSinceLastTraining = 0;
	private int placementType = 0;
	private InfluxDBTransitionsClient influxDBTransitionsClient;

	public NeuralNetworksBasedActorCriticModel(
		int nCpus,
		int nVertices,
		String retentionPolicyName,
		NeuralNetworkConfiguration neuralNetworkConfiguration,
		Logger log) {

		this.nCpus = nCpus;
		this.nVertices = nVertices;
		this.log = log;
		checkNotNull(neuralNetworkConfiguration);
		this.seed = neuralNetworkConfiguration.getSeed();
		this.rand = new Random(this.seed);
		this.numInputs = neuralNetworkConfiguration.getNumInputs();
		this.numOutputs = neuralNetworkConfiguration.getNumOutputs();
		this.nEpochs = neuralNetworkConfiguration.getNumEpochs();
		this.learningRate = neuralNetworkConfiguration.getLearningRate();
		this.epsilonGreedyThreshold = neuralNetworkConfiguration.getEpsilonGreedyThreshold();
		this.diminishingGreedyThreshold = 0.9;
		this.trainTriggerThreshold = neuralNetworkConfiguration.getTrainTriggerThreshold();
		this.numHiddenNodes = neuralNetworkConfiguration.getNumHiddenNodes();
		this.maxTrainingCacheSize = neuralNetworkConfiguration.getMaxTrainingCacheSize();
		this.scoreIterationPrintFrequency = neuralNetworkConfiguration.getNumEpochs();
		setupNeuralNetwork();
		setupInfluxDBConnection(retentionPolicyName);
	}

	private void setupNeuralNetwork() {
		conf = getDeepDenseLayerNetworkConfiguration();
		//Create the network
		net = new MultiLayerNetwork(conf);
		net.init();
		net.setListeners(new ScoreIterationListener(scoreIterationPrintFrequency));
		trainingCache = CacheBuilder.newBuilder()
			.maximumSize(maxTrainingCacheSize)
			.expireAfterWrite(1, TimeUnit.HOURS)
			.build();
	}

	private void setupInfluxDBConnection(String retentionPolicyName) {
		influxDBTransitionsClient = new InfluxDBTransitionsClient(
			"http://127.0.0.1:8086",
			"flink-transitions", retentionPolicyName, log);
		influxDBTransitionsClient.setup();
	}

	public void updateTrainingData(
		List<Integer> placement,
		List<Double> cpuUsageMetrics,
		Double arrivalRate,
		Double throughput,
		List<Double> proxyNumaDistances) {

		updateTrainingCache(placement, cpuUsageMetrics, arrivalRate, throughput);
		updatesSinceLastTraining++;
		if (updatesSinceLastTraining >= trainTriggerThreshold) {
			INDArray inputArray = null;
			INDArray labelArray = null;
			for (Map.Entry<INDArray, INDArray> entry : trainingCache.asMap().entrySet()) {
				if (inputArray == null) {
					inputArray = entry.getKey();
				} else {
					inputArray = Nd4j.vstack(inputArray, entry.getKey());
				}
				if (labelArray == null) {
					labelArray = entry.getValue().reshape(1, 1);
				} else {
					labelArray = Nd4j.vstack(labelArray, entry.getValue().reshape(1, 1));
				}
			}
			if (inputArray != null && inputArray.shape()[0] >= trainTriggerThreshold) {
				train(inputArray, labelArray);
				trainingCache.invalidateAll();
			}
			updatesSinceLastTraining = 0;
		}
		influxDBTransitionsClient.writeInputDataPoint(
			placement.toString(),
			cpuUsageMetrics.toString(),
			arrivalRate,
			throughput,
			placementType,
			proxyNumaDistances.toString());
	}

	private void updateTrainingCache(
		List<Integer> placement,
		List<Double> cpuUsageMetrics,
		Double arrivalRate,
		Double throughput) {
		INDArray cpuMetricsArr = Nd4j.createFromArray(cpuUsageMetrics.toArray(new Double[0]));
		INDArray arrivalRateArr = Nd4j.createFromArray(arrivalRate);
		INDArray encodedPlacement = encodePlacement(placement);
		long actualInputSize =
			encodedPlacement.length() + cpuMetricsArr.length() + arrivalRateArr.length();
		if (actualInputSize != numInputs) {
			log.warn(
				"Incorrect number of input features. Expected:{}, "
					+ "Found:{} [Encoded placement length:{}, CPU Metrics length {}, "
					+ "Edge flow rates length {}]",
				numInputs,
				actualInputSize,
				encodedPlacement.length(),
				cpuMetricsArr.length(),
				arrivalRateArr.length());
		} else {
			trainingCache.put(
				Nd4j.hstack(
					encodedPlacement.reshape(1, encodedPlacement.length()),
					cpuMetricsArr.reshape(1, cpuMetricsArr.length()),
					arrivalRateArr.reshape(1, 1)),
				Nd4j.createFromArray(throughput));
		}
	}

	private void train(INDArray inputData, INDArray labels) {
		for (int i = 0; i < nEpochs; i++) {
			net.fit(inputData, labels);
		}
		isTrained = true;
	}

	private INDArray predict(INDArray input) {
		return net.output(input, false);
	}

	private MultiLayerConfiguration getDeepDenseLayerNetworkConfiguration() {
		return new NeuralNetConfiguration.Builder()
			.seed(seed)
			.weightInit(WeightInit.XAVIER)
			.updater(new Nesterovs(learningRate, 0.9))
			.list()
			.layer(new DenseLayer.Builder().nIn(numInputs).nOut(numHiddenNodes)
				.activation(Activation.TANH).build())
			.layer(new DenseLayer.Builder().nIn(numHiddenNodes).nOut(numHiddenNodes)
				.activation(Activation.TANH).build())
			.layer(new OutputLayer.Builder(LossFunctions.LossFunction.MSE)
				.activation(Activation.IDENTITY)
				.nIn(numHiddenNodes).nOut(numOutputs).build())
			.build();
	}

	public List<Integer> selectAction(
		Map<List<Integer>, Double> suggestedActions,
		List<Double> cpuUsageMetrics,
		Double arrivalRate) {

		List<Integer> placementSuggestion;
		double epsilonGreedyScore = rand.nextDouble();
		if (epsilonGreedyScore > diminishingGreedyThreshold) {
			if (isTrained) {
				log.info("Predicting using {} suggested actions ", suggestedActions.size());
				List<Double> predictedValues = new ArrayList<>();
				INDArray cpuMetricsArr = Nd4j.createFromArray(cpuUsageMetrics.toArray(new Double[0]));
				INDArray arrivalRateArr = Nd4j.createFromArray(arrivalRate);
				for (List<Integer> cpuIdList : suggestedActions.keySet()) {
					INDArray encodedPlacementActions = encodePlacement(cpuIdList);
					INDArray inputArray = Nd4j.hstack(
						encodedPlacementActions.reshape(1, encodedPlacementActions.length()),
						cpuMetricsArr.reshape(1, cpuMetricsArr.length()),
						arrivalRateArr.reshape(1, 1));
					predictedValues.add(predict(inputArray).toDoubleVector()[0]);
				}
				double predictedMaxThroughput = predictedValues
					.stream()
					.max(Comparator.naturalOrder())
					.orElse(0.0);
				int argMax = predictedValues.indexOf(predictedMaxThroughput);
				if (argMax >= 0) {
					List<List<Integer>> suggestedActionList = new ArrayList<>(suggestedActions.keySet());
					placementSuggestion = suggestedActionList.get(argMax);
					log.info("Suggested actions : {}", suggestedActionList);
					log.info("Predicted values: {}", predictedValues);
					log.info(
						"Suggesting action with predicted throughput of {} : {} ",
						predictedMaxThroughput, placementSuggestion);
					placementType = PLACEMENT_TYPE_PREDICTED;
					return placementSuggestion;
				}
			}
		} else {
			if (diminishingGreedyThreshold > epsilonGreedyThreshold) {
				diminishingGreedyThreshold -= DIMINISHING_STEP_SIZE;
			}
			placementSuggestion = getRandomPlacementAction();
			log.info("Suggesting random action {}", placementSuggestion);
			placementType = PLACEMENT_TYPE_RANDOM;
			return placementSuggestion;
		}

		placementType = PLACEMENT_TYPE_NAIVE;
		return suggestedActions
			.entrySet()
			.stream()
			.max(Map.Entry.comparingByValue())
			.map(Map.Entry::getKey)
			.orElse(null);
	}

	private List<Integer> getRandomPlacementAction() {
		List<Integer> cpuAssignmentIds = IntStream
			.range(0, nCpus)
			.boxed()
			.collect(Collectors.toList());
		Collections.shuffle(cpuAssignmentIds, rand);

		return cpuAssignmentIds.subList(0, nVertices);
	}

	private INDArray encodePlacement(List<Integer> placementAction) {
		double[][] encodedPlacement = new double[nVertices][nCpus];
		int operatorIndex = 0;
		for (Integer cpuAssignment : placementAction) {
			if (cpuAssignment != 0) {
				double[] currentOperatorPlacement = new double[nCpus];
				currentOperatorPlacement[cpuAssignment] = 1.0d;
				encodedPlacement[operatorIndex++] = currentOperatorPlacement;
			}
		}
		return Nd4j.createFromArray(encodedPlacement);
	}

}
