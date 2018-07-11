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
package com.netflix.spinnaker.q.sqs.federation

import com.netflix.spinnaker.q.DeadMessageCallback
import com.netflix.spinnaker.q.Message
import com.netflix.spinnaker.q.QueueCallback
import com.netflix.spinnaker.q.metrics.EventPublisher
import com.netflix.spinnaker.q.metrics.MonitorableQueue
import com.netflix.spinnaker.q.metrics.QueueState
import com.netflix.spinnaker.q.sqs.SqsQueue
import java.time.temporal.TemporalAmount

class FederatedSqsQueue(
  private val federationManager: FederationManager,
  override val publisher: EventPublisher,
  override val ackTimeout: TemporalAmount,
  override val deadMessageHandlers: List<DeadMessageCallback>
) : MonitorableQueue {

  override fun poll(callback: QueueCallback) {
    queue().poll(callback)
  }

  override fun push(message: Message, delay: TemporalAmount) {
    queue(message).push(message, delay)
  }

  override fun reschedule(message: Message, delay: TemporalAmount) {
    queue(message).reschedule(message, delay)
  }

  override fun ensure(message: Message, delay: TemporalAmount) {
    queue(message).ensure(message, delay)
  }
  override fun readState(): QueueState =
    federationManager.queues().values.fold(QueueState(0, 0, 0, 0)) { agg, queue ->
      queue.readState().let {
        agg.copy(
          depth = agg.depth + it.depth,
          ready = agg.ready + it.ready,
          unacked = agg.unacked + it.unacked,
          orphaned = agg.orphaned + agg.orphaned
        )
      }
    }

  override fun containsMessage(predicate: (Message) -> Boolean): Boolean {
    throw UnsupportedOperationException("not implemented")
  }

  private fun queue(message: Message? = null): SqsQueue {
    val partition = if (message == null) {
      federationManager.partition()
    } else {
      federationManager.partition(message)
    }
    return federationManager.queues()[partition]
      ?: throw IllegalStateException("Missing internal queue for partition: $partition")
  }
}
