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

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * InfluxDB metrics client to access scheduling metrics.
 */
public class InfluxDBTransitionsClient {
	private final String serverURL;
	private final String databaseName;
	private final Logger log;
	private InfluxDB influxDB;
	private final int readMetricIntervalInMinutes = 1;

	public InfluxDBTransitionsClient(String serverURL, String databaseName, Logger log) {
		this.serverURL = serverURL;
		this.databaseName = databaseName;
		this.log = log;
	}

	public void setup() {
		if (influxDB == null) {
			influxDB = InfluxDBFactory.connect(serverURL);
			influxDB.setDatabase(databaseName);
		}
	}

	public void writeToDB(String action, String oldState, String newState, Double reward) {
		try {
			influxDB.write(Point
				.measurement("state_transitions")
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

	public Map<String, Double> getCpuMetrics(int nCpus) {
		Map<String, Double> resultMap = new HashMap<>();
		for (int i = 0; i < nCpus; i++) {
			int cpuId = i;
			try {
				QueryResult queryResult = influxDB.query(new Query(
					"SELECT value FROM taskmanager_System_CPU_UsageCPU" + i
						+ " WHERE time > now() - " + 3 + "m ORDER BY time DESC LIMIT 1"));
				List<QueryResult.Result> results = queryResult.getResults();
				results.forEach(result -> {
					List<QueryResult.Series> series = result.getSeries();
					if (series != null && !series.isEmpty()) {
						List<Object> record = series.get(0).getValues().get(0);
						resultMap.put(
							String.valueOf(cpuId),
							(Double) record.get(1) / 100.0);
					}
				});
			} catch (Exception e) {
				log.warn(
					"Exception occurred when retrieving metrics for CPU {} : {}",
					cpuId,
					e.getMessage());
			}
		}
		return resultMap;
	}

	public Map<String, Double> getOperatorUsageMetrics() {
		Map<String, Double> resultMap = new HashMap<>();
		try {
			QueryResult queryResult = influxDB.query(new Query(
				"SELECT MEAN(value) FROM taskmanager_job_task_operator_currentCpuUsage WHERE time > now() - "
					+ readMetricIntervalInMinutes
					+ "m GROUP BY operator_id, subtask_index"));
			List<QueryResult.Result> results = queryResult.getResults();
			results.forEach(result -> {
				List<QueryResult.Series> series = result.getSeries();
				if (series != null && !series.isEmpty()) {
					series.forEach(seriesElement -> {
						List<Object> record = seriesElement.getValues().get(0);
						if (record.size() == 2) {
							resultMap.put(
								seriesElement.getTags().get("operator_id") + "_" + seriesElement
									.getTags()
									.get("subtask_index"),
								((Double) record.get(1)) / 1000000000.0);
						} else {
							log.warn("Size mismatch when reading metric record.");
						}
					});
				}
			});
		} catch (Exception e) {
			log.warn(
				"Exception occurred when retrieving metrics for operator CPU usage : {}",
				e.getMessage());
		}
		return resultMap;
	}

	public void closeConnection() {
		influxDB.close();

	}
}
