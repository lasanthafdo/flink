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

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.AtomicDouble;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * InfluxDB metrics client to access scheduling metrics.
 */
public class InfluxDBMetricsClient {
	private final String serverURL;
	private final String databaseName;
	private final Logger log;
	private InfluxDB influxDB;

	public InfluxDBMetricsClient(String serverURL, String databaseName, Logger log) {
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

	public Map<String, Double> getRateMetricsFor(
		String metricName,
		String keyField,
		String valueField) {

		Map<String, Double> resultMap = new HashMap<>();
		try {
			QueryResult queryResult = influxDB.query(new Query(
				"SELECT LAST(" + valueField + ") FROM " + metricName + " GROUP BY " + keyField));
			List<QueryResult.Result> results = queryResult.getResults();
			results.forEach(result -> {
				List<QueryResult.Series> series = result.getSeries();
				if (series != null && !series.isEmpty()) {
					series.forEach(seriesElement -> {
						List<Object> record = seriesElement.getValues().get(0);
						if (record.size() == 2) {
							resultMap.put(
								seriesElement.getTags().get(keyField),
								BigDecimal
									.valueOf((Double) record.get(1))
									.setScale(2, RoundingMode.HALF_UP)
									.doubleValue());
						} else {
							log.warn("Size mismatch when reading metric record.");
						}
					});
				}
			});
		} catch (Exception e) {
			log.warn(
				"Exception occurred when retrieving metric {} with key '{}' and value '{}': {}",
				metricName,
				keyField,
				valueField,
				e.getMessage());
		}
		return resultMap;
	}

	public Map<String, Double> getCpuMetrics(int nCpus) {
		Map<String, Double> resultMap = new HashMap<>();
		for (int i = 0; i < nCpus; i++) {
			int cpuId = i;
			try {
				QueryResult queryResult = influxDB.query(new Query(
					"SELECT LAST(value) FROM taskmanager_System_CPU_UsageCPU" + i));
				List<QueryResult.Result> results = queryResult.getResults();
				results.forEach(result -> {
					List<QueryResult.Series> series = result.getSeries();
					if (series != null && !series.isEmpty()) {
						List<Object> record = series.get(0).getValues().get(0);
						resultMap.put(
							String.valueOf(cpuId),
							BigDecimal
								.valueOf((Double) record.get(1) / 100.0)
								.setScale(3, RoundingMode.HALF_UP)
								.doubleValue());
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
				"SELECT LAST(value) FROM taskmanager_job_task_operator_currentCpuUsage GROUP BY operator_id, subtask_index"));
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
								BigDecimal.valueOf((Double) record.get(1) / 1000000000.0)
									.setScale(3, RoundingMode.HALF_UP)
									.doubleValue());
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

	public Double getMostRecentArrivalRate(List<String> sourceVertexIDs) {
		final AtomicDouble arrivalRate = new AtomicDouble(0.0);
		for (String sourceVertexID : sourceVertexIDs) {
			try {
				String[] sourceVertexIDComponents = sourceVertexID.split("_");
				QueryResult queryResult = influxDB.query(new Query(
					"SELECT LAST(rate) FROM taskmanager_job_task_operator_numRecordsOutPerSecond "
						+ "WHERE operator_id = '" + sourceVertexIDComponents[0]
						+ "' AND subtask_index = '" + sourceVertexIDComponents[1] + "'"));
				List<QueryResult.Result> results = queryResult.getResults();
				results.forEach(result -> {
					List<QueryResult.Series> series = result.getSeries();
					if (series != null && !series.isEmpty()) {
						List<Object> record = series.get(0).getValues().get(0);
						arrivalRate.addAndGet((Double) record.get(1));
					}
				});
			} catch (Exception e) {
				log.warn(
					"Exception occurred when retrieving metrics for source vertex with ID {} : {}",
					sourceVertexID,
					e.getMessage());
			}
		}

		return arrivalRate.get();
	}

	public void closeConnection() {
		influxDB.close();
	}
}
