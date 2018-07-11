/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.q.sqs

import com.netflix.spinnaker.q.Message
import java.time.Instant
import java.time.temporal.TemporalAmount

/**
 * Provides scheduling and deduplication utilities for SQS messages.
 */
interface MessageRepository {
  /**
   * Given SQS [messageIds], attempt to acquire non-reentrant locks for
   * the provided [ttl] duration.
   *
   * This is used for deduplication of received messages, but does not restrict
   * processing of the same Message fingerprint more than once. There is no
   * explicit remove interface for these locks, so the internal storage impl
   * must be able to age-out old locks that have gone past their [ttl].
   *
   * Returns a filtered list of [messageIds] which have locks acquired.
   */
  fun tryAcquireLocks(messageIds: List<String>, ttl: TemporalAmount): List<String>

  /**
   * Schedules a [message] for delivery after [delay].
   */
  fun schedule(message: Message, delay: TemporalAmount)

  /**
   * Given a list of [fingerprints], a map of any matching reschedules and their
   * delivery times will be returned. Any fingerprint that does not have a
   * reschedule record will not be included in the response.
   */
  fun schedules(fingerprints: List<String>): Map<String, Instant>

  /**
   * For every [Message] whose scheduled delivery time is sooner than
   * [maxDeliveryTime], invoke [callback].
   */
  fun scheduled(maxDeliveryTime: Instant, callback: (Message, Instant) -> Unit)
}
