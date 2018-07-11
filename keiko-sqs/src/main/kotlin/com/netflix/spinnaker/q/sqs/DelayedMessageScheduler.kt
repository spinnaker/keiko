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

import com.netflix.spinnaker.q.Queue
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.temporal.TemporalAmount

/**
 * Enqueues delayed messages onto the provided queue.
 */
interface DelayedMessageScheduler {
  fun schedule()
}

class DefaultDelayedMessageScheduler(
  private val messageRepository: MessageRepository,
  private val queue: Queue,
  private val maxDeliveryDelay: TemporalAmount
) : DelayedMessageScheduler {

  private val log = LoggerFactory.getLogger(javaClass)

  override fun schedule() {
    val now = Instant.now()
    messageRepository.scheduled(now.plus(maxDeliveryDelay)) { message, deliveryTime ->
      val delay = (deliveryTime.toEpochMilli() - now.toEpochMilli()).let {
        if (it <= 0) {
          Duration.ZERO
        } else {
          Duration.ofMillis(it)
        }
      }
      log.info("Enqueuing scheduled message (delay $delay): $message")
      queue.push(message, delay)
    }
  }
}
