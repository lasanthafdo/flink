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

package org.apache.flink.runtime.executiongraph.metrics;

import org.apache.flink.metrics.Gauge;

import java.util.function.Supplier;

/**
 * A gauge that returns the cpu ID for an operator.
 */
public class CpuIdGauge implements Gauge<Integer> {

	// ------------------------------------------------------------------------

	private Integer lastCpuId = -1;
	private Supplier<Integer> cpuIdSupplier;

	// ------------------------------------------------------------------------

	@Override
	public Integer getValue() {
		if (cpuIdSupplier != null) {
			lastCpuId = cpuIdSupplier.get();
		}
		return lastCpuId;
	}

	public void setCpuIdSupplier(Supplier<Integer> cpuIdSupplier) {
		this.cpuIdSupplier = cpuIdSupplier;
	}
}
