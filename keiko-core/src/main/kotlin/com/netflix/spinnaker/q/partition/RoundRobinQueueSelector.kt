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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.google.common.hash.Hashing
import com.netflix.spinnaker.q.Message
import com.netflix.spinnaker.q.Queue
import java.lang.Math.abs
import java.nio.charset.StandardCharsets.UTF_8

/**
 * Delegates to a pool of queues, reading messages out in a round robin order.
 */
class RoundRobinQueueSelector(
  private val queues: List<Queue>,
  fallbackPartitionProvider: RoundRobinPartitionProvider? = null
) : QueueSelector {

  companion object {
    private val hashObjectMapper = ObjectMapper().apply {
      enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
    }

    private val defaultPartitionProvider: RoundRobinPartitionProvider = { message ->
      hashObjectMapper.convertValue(message, MutableMap::class.java)
        .apply { remove("attributes") }
        .let {
          Hashing
            .murmur3_32()
            .hashString(hashObjectMapper.writeValueAsString(it), UTF_8)
            .asInt()
        }
    }
  }

  private var iterable: Iterator<Queue> = queues.iterator()
  private val size = queues.size
  private val partitionProvider = fallbackPartitionProvider ?: defaultPartitionProvider

  init {
    if (queues.isEmpty()) {
      throw IllegalArgumentException("At least one queue must be provided")
    }
  }

  override fun forPoll(): Queue {
    if (!iterable.hasNext()) {
      iterable = queues.iterator()
    }
    return iterable.next()
  }

  override fun forMessage(message: Message) = queues[message.partition()]

  private fun Message.partition(): Int =
    (getAttribute<PartitionAttribute>()?.id?.hashCode() ?: partitionProvider(this))
      .let { abs(it % size) }
}

typealias RoundRobinPartitionProvider = (m: Message) -> Int
