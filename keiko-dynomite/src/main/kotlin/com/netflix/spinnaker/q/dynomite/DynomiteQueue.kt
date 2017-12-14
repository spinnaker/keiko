/*
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.spinnaker.q.dynomite

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.hash.Hashing
import com.netflix.dyno.queues.DynoQueue
import com.netflix.dyno.queues.redis.RedisQueues
import com.netflix.spinnaker.q.AttemptsAttribute
import com.netflix.spinnaker.q.MaxAttemptsAttribute
import com.netflix.spinnaker.q.Message
import com.netflix.spinnaker.q.Queue
import com.netflix.spinnaker.q.QueueCallback
import com.netflix.spinnaker.q.metrics.EventPublisher
import com.netflix.spinnaker.q.metrics.MessageDead
import com.netflix.spinnaker.q.metrics.MonitorableQueue
import com.netflix.spinnaker.q.metrics.QueueState
import com.netflix.spinnaker.q.metrics.fire
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.nio.charset.Charset
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAmount
import java.util.concurrent.TimeUnit

private const val DYNO_PRIMARY_QUEUE = "primary"

// TODO rz - Currently only supports a single dyno queue shard
// TODO rz - Remove MonitorableQueue interface?
class DynomiteQueue(
  private val redisQueues: RedisQueues,
  private val mapper: ObjectMapper,
  override val ackTimeout: TemporalAmount = Duration.ofMinutes(1),
  override val deadMessageHandler: (Queue, Message) -> Unit,
  override val publisher: EventPublisher
) : MonitorableQueue, Closeable {

  private val log = LoggerFactory.getLogger(javaClass)

  // The queue implementation exposes a decent amount of metrics own its own.
  override fun readState() = QueueState(
      depth = queue().size().toInt(),
      ready = 0,
      unacked = 0,
      orphaned = 0,
      hashDrift = 0
    )

  override fun poll(callback: QueueCallback) {
    // TODO rz - seems to be a bug in waiting with a clock
    queue().pop(1, 0, TimeUnit.SECONDS)
      .firstOrNull()
      ?.also { message ->
        readMessage(message) { msg ->
          val attempts = msg.getAttribute<AttemptsAttribute>()?.attempts ?: 0
          val maxAttempts = msg.getAttribute<MaxAttemptsAttribute>()?.maxAttempts ?: 0

          if (maxAttempts > 0 && attempts > maxAttempts) {
            log.warn("Message ${msg.hash()} with payload $msg exceeded $maxAttempts retries")
            deadMessageHandler.invoke(this, msg)
            fire<MessageDead>()
          } else {
            callback(msg, { queue().ack(listOf(message)) })
          }
        }
      }
  }

  override fun push(message: Message, delay: TemporalAmount) {
    val m = com.netflix.dyno.queues.Message(message.hash(), mapper.writeValueAsString(message)).apply {
      timeout = delay.get(ChronoUnit.SECONDS) * 1000
    }
    queue().push(listOf(m))
  }

  override fun reschedule(message: Message, delay: TemporalAmount) {
    queue().remove(message.hash())
    push(message, delay)
  }

  private fun readMessage(message: com.netflix.dyno.queues.Message, block: (Message) -> Unit) {
    val json = message.payload as String?
    if (json == null) {
      log.error("Payload for message ${message.id} is missing")
      queue().remove(message.id)
    } else {
      try {
        val m = mapper.readValue(message.payload, Message::class.java)
          .apply {
            // TODO: AttemptsAttribute could replace `attemptsKey`
            val currentAttempts = (getAttribute() ?: AttemptsAttribute())
              .run { copy(attempts = attempts + 1) }
            setAttribute(currentAttempts)
          }

        block.invoke(m)
      } catch (e: IOException) {
        log.error("Failed to read message ${message.id}, requeuing...", e)
        queue().push(listOf(message))
      }
    }
  }

  private fun queue(): DynoQueue = redisQueues.get(DYNO_PRIMARY_QUEUE)

  private fun Message.hash() =
    Hashing
      .murmur3_128()
      .hashString(toString(), Charset.defaultCharset())
      .toString()

  override fun close() {
    redisQueues.close()
  }
}
