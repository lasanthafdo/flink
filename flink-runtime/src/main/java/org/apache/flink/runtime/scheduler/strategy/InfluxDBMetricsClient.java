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

package org.apache.flink.runtime.scheduler.strategy;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

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

	InfluxDBMetricsClient(String serverURL, String databaseName) {
		this.serverURL = serverURL;
		this.databaseName = databaseName;
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
		QueryResult queryResult = influxDB.query(new Query(
			"SELECT " + keyField + "," + valueField + " FROM " + metricName));
		List<QueryResult.Result> results = queryResult.getResults();
		results.forEach(result -> {
			List<QueryResult.Series> series = result.getSeries();
			if(series != null && !series.isEmpty()) {
				series.get(0).getValues().forEach(record -> {
					if(record.size() == 3) {
						resultMap.put(record.get(1).toString(), Double.valueOf(record.get(2).toString()));
					}
				});
			}
		});
		return resultMap;
	}

	public void closeConnection() {
		influxDB.close();

	}
}
