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

public class NeuralNetworkConfiguration {
	private int numEpochs;
	private int numInputs;
	private int numOutputs = 1;
	private final long seed;
	private double learningRate;
	private double epsilonGreedyThreshold;
	private int trainTriggerThreshold;

	public NeuralNetworkConfiguration() {
		this(100, 680, 0.01, 0.3, 10);
	}

	public NeuralNetworkConfiguration(
		int numEpochs,
		long seed,
		double learningRate,
		double epsilonGreedyThreshold,
		int trainTriggerThreshold) {

		this.numEpochs = numEpochs;
		this.seed = seed;
		this.learningRate = learningRate;
		this.epsilonGreedyThreshold = epsilonGreedyThreshold;
		this.trainTriggerThreshold = trainTriggerThreshold;
	}

	public int getNumEpochs() {
		return numEpochs;
	}

	public NeuralNetworkConfiguration setNumEpochs(int numEpochs) {
		this.numEpochs = numEpochs;
		return this;
	}

	public int getNumInputs() {
		return numInputs;
	}

	public NeuralNetworkConfiguration setNumInputs(int numInputs) {
		this.numInputs = numInputs;
		return this;
	}

	public int getNumOutputs() {
		return numOutputs;
	}

	public NeuralNetworkConfiguration setNumOutputs(int numOutputs) {
		this.numOutputs = numOutputs;
		return this;
	}

	public long getSeed() {
		return seed;
	}

	public double getLearningRate() {
		return learningRate;
	}

	public NeuralNetworkConfiguration setLearningRate(double learningRate) {
		this.learningRate = learningRate;
		return this;
	}

	public double getEpsilonGreedyThreshold() {
		return epsilonGreedyThreshold;
	}

	public NeuralNetworkConfiguration setEpsilonGreedyThreshold(double epsilonGreedyThreshold) {
		this.epsilonGreedyThreshold = epsilonGreedyThreshold;
		return this;
	}

	public int getTrainTriggerThreshold() {
		return trainTriggerThreshold;
	}

	public NeuralNetworkConfiguration setTrainTriggerThreshold(int trainTriggerThreshold) {
		this.trainTriggerThreshold = trainTriggerThreshold;
		return this;
	}
}
