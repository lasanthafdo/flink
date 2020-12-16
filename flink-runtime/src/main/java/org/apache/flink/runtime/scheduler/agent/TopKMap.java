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

import java.util.HashMap;
import java.util.Optional;

public class TopKMap<K, V extends Comparable<V>> extends HashMap<K, V> {

	private static final long serialVersionUID = 6476441707408416765L;
	private final int maxSize;

	public TopKMap(int maxSize) {
		super(maxSize);
		this.maxSize = maxSize;
	}

	@Override
	public V put(K key, V value) {
		if (this.size() == maxSize) {
			Optional<Entry<K, V>> optionalMinEntry = this
				.entrySet()
				.stream()
				.min(Entry.comparingByValue());
			if (optionalMinEntry.isPresent()) {
				Entry<K, V> minEntry = optionalMinEntry.get();
				if (value.compareTo(minEntry.getValue()) > 0) {
					super.remove(minEntry.getKey());
				} else {
					return null;
				}
			}
		}
		return super.put(key, value);
	}
}
