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
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * InfluxDB metrics client to access scheduling metrics.
 */
public class InfluxDBMetricsClient {
	private final String serverURL;
	private final String databaseName;
	private InfluxDB influxDB;
	private Logger log;

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
				"SELECT MEAN(" + valueField + ") FROM " + metricName + " WHERE time > now() - " + 3
					+ "m GROUP BY " + keyField));
			List<QueryResult.Result> results = queryResult.getResults();
			results.forEach(result -> {
				List<QueryResult.Series> series = result.getSeries();
				if (series != null && !series.isEmpty()) {
					series.forEach(seriesElement -> {
						List<Object> record = seriesElement.getValues().get(0);
						if (record.size() == 2) {
							resultMap.put(
								seriesElement.getTags().get(keyField),
								(Double) record.get(1));
						} else {
							log.warn("Size mismatch when reading metric record.");
						}
					});
				}
			});
		} catch (Exception e) {
			log.warn(
				"Exception occured when retrieving metric {} with key '{}' and value '{}': {}",
				metricName,
				keyField,
				valueField,
				e.getMessage());
		}
		return resultMap;
	}

	public Map<Integer, Double> getCpuMetrics(int nCpus) {
		Map<Integer, Double> resultMap = new HashMap<>();
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
							cpuId,
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

	public void closeConnection() {
		influxDB.close();

	}
}
