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

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * InfluxDB metrics client to access scheduling metrics.
 */
public class InfluxDBTransitionsClient {
	private final String serverURL;
	private final String databaseName;
	private final String retentionPolicy;
	private final Logger log;
	private InfluxDB influxDB;

	public InfluxDBTransitionsClient(
		String serverURL, String databaseName, String retentionPolicyName, Logger log) {
		this.serverURL = serverURL;
		this.databaseName = databaseName;
		this.retentionPolicy = retentionPolicyName;
		this.log = log;
	}

	public void setup() {
		if (influxDB == null) {
			influxDB = InfluxDBFactory.connect(serverURL);
			influxDB.setDatabase(databaseName);
			influxDB.setRetentionPolicy(retentionPolicy);
			influxDB.setConsistency(InfluxDB.ConsistencyLevel.ANY);
		}
	}

	public void writeQLearningActionToDB(
		String action,
		String oldState,
		String newState,
		Double reward) {
		try {
			influxDB.write(Point
				.measurement("state_transitions")
				.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
				.tag("host", "127.0.0.1")
				.addField("action", action)
				.addField("oldState", oldState)
				.addField("newState", newState)
				.addField("reward", reward)
				.build());
		} catch (Exception e) {
			log.warn("Exception occurred when writing state transitions to DB: {}", e.getMessage());
		}
	}

	public void writeInputDataPoint(
		String placementAction,
		String cpuUsageMetrics,
		Double arrivalRate,
		Double throughput,
		Integer placementType,
		String proxyNumaDistances) {
		try {
			influxDB.write(Point
				.measurement("state_snapshots")
				.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
				.tag("host", "127.0.0.1")
				.addField("placementAction", placementAction)
				.addField("cpuUsageMetrics", cpuUsageMetrics)
				.addField("arrivalRate", arrivalRate)
				.addField("throughput", throughput)
				.addField("placementType", placementType)
				.addField("proxyNumaDistances", proxyNumaDistances)
				.build());
		} catch (Exception e) {
			log.warn("Exception occurred when writing state snapshot to DB: {}", e.getMessage());
		}
	}

	public void closeConnection() {
		influxDB.close();
	}
}
