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
package com.netflix.spinnaker.q.partition

import com.netflix.spinnaker.q.DeadMessageCallback
import com.netflix.spinnaker.q.Message
import com.netflix.spinnaker.q.Queue
import com.netflix.spinnaker.q.QueueCallback
import java.time.temporal.TemporalAmount

/**
 * Delegates to a [QueueSelector] to allow transparent queue partitioning.
 *
 * Note: This queue type supercedes any values for [queueName], [ackTimeout], and
 * [deadMessageHandlers] that may be provided by the underlying queues defined
 * within [QueueSelector].
 */
class PartitionedQueue(
  private val queueSelector: QueueSelector,
  override val ackTimeout: TemporalAmount,
  override val deadMessageHandlers: List<DeadMessageCallback>
) : Queue {

  override fun poll(callback: QueueCallback) {
    queueSelector.forPoll().poll(callback)
  }

  override fun push(message: Message, delay: TemporalAmount) {
    queueSelector.forMessage(message).push(message, delay)
  }

  override fun reschedule(message: Message, delay: TemporalAmount) {
    queueSelector.forMessage(message).reschedule(message, delay)
  }

  override fun ensure(message: Message, delay: TemporalAmount) {
    queueSelector.forMessage(message).ensure(message, delay)
  }
}
