/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link SlotMatchingStrategy} which picks the first matching slot with requested location.
 */
public enum LocationBasedSlotMatchingStrategy implements SlotMatchingStrategy {
	INSTANCE;

	private static final Logger log = LoggerFactory.getLogger(LocationBasedSlotMatchingStrategy.class);

	@Override
	public <T extends TaskManagerSlotInformation> Optional<T> findMatchingSlot(
		ResourceProfile requestedProfile,
		Collection<T> freeSlots,
		Function<InstanceID, Integer> numberRegisteredSlotsLookup) {
		String requestedLocation = requestedProfile.getResourceLocation();
		if (requestedLocation.isEmpty()) {
			log.debug("Requested location is empty");
			return freeSlots
				.stream()
				.filter(slot -> slot.isMatchingRequirement(requestedProfile))
				.findAny();
		} else {
			Optional<T> matchingFreeSlot = freeSlots
				.stream()
				.filter(slot -> slot.isMatchingRequirement(requestedProfile)
					&& requestedLocation.equals(slot.getResourceProfile().getResourceLocation()))
				.findAny();
			if (log.isDebugEnabled()) {
				if (matchingFreeSlot.isPresent()) {
					log.debug(
						"Found matching free slot at {} for request at {}",
						matchingFreeSlot.get().getResourceProfile().getResourceLocation(),
						requestedLocation);
				} else {
					log.warn(
						"Could not find matching slot for request at {}. Free slots {}",
						requestedLocation,
						freeSlots.stream().collect(
							Collectors.toMap(
								TaskManagerSlotInformation::getSlotId,
								slot -> slot.getResourceProfile().getResourceLocation())));
				}
			}
			return matchingFreeSlot;
		}
	}
}
