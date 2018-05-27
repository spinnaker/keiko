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

import com.netflix.spinnaker.q.Message
import com.netflix.spinnaker.q.Queue
import java.security.SecureRandom

/**
 * Allows messages to be prioritized across weighted queues.
 *
 * A [PartitionWeightMapper] can be provided to translate between application-specific priorities
 * and the [WeightedQueue] weight value.
 */
class PriorityQueueSelector(
  private val queues: List<WeightedQueue>,
  customPartitionWeightMapper: PartitionWeightMapper? = null
) : QueueSelector {

  companion object {
    private val defaultPartitionWeightMapper: PartitionWeightMapper = { partitionAttribute ->
      partitionAttribute.id.toIntOrNull()
    }
  }

  private val rand = SecureRandom()
  private val weightSum = queues.map { it.weight }.sum()
  private val highestPriorityQueue: Queue
  private val defaultQueue: Queue
  private val partitionWeightMapper = customPartitionWeightMapper ?: defaultPartitionWeightMapper

  init {
    if (queues.isEmpty()) {
      throw IllegalArgumentException("At least one queue must be provided")
    }
    if (queues.filter { it.default }.size != 1) {
      throw IllegalArgumentException("One queue must be flagged as default")
    }
    highestPriorityQueue = queues.sortedByDescending { it.weight }.first().queue
    defaultQueue = queues.first { it.default }.queue
  }

  override fun forPoll() = selectQueue(rand.nextInt(weightSum), queues.iterator())

  private tailrec fun selectQueue(r: Int, iterator: Iterator<WeightedQueue>): Queue {
    if (!iterator.hasNext()) {
      // We should never get here, but if we do, just read off the highest priority queue
      return highestPriorityQueue
    }
    val q = iterator.next()
    if ((r - q.weight) >= 0) {
      return q.queue
    }
    return selectQueue(r, iterator)
  }

  override fun forMessage(message: Message): Queue {
    val partition = message.getAttribute<PartitionAttribute>()
      ?.let { partitionWeightMapper(it) }
      ?: return defaultQueue

    return queues.firstOrNull { it.weight == partition }?.queue ?: defaultQueue
  }
}

/**
 * @param queue the actual queue implementation
 * @param weight the probability the queue will be selected; higher number has a higher priority
 * @param default if true, this queue will be chosen on writes if a [PartitionAttribute] is
 *                unavailable or does not match any other queues
 */
data class WeightedQueue(
  val queue: Queue,
  val weight: Int,
  val default: Boolean
)

/**
 * Provides a way for applications to map arbitrarily named priorities to the weight values
 * associated with a weighted queue. If the mapper cannot map a particular [PartitionAttribute],
 * and has no definition of a default value, null should be returned.
 */
typealias PartitionWeightMapper = (PartitionAttribute) -> Int?
