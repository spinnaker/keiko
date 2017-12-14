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
import com.netflix.dyno.jedis.DynoJedisClient
import com.netflix.spinnaker.q.DeadMessageCallback
import com.netflix.spinnaker.q.Message
import com.netflix.spinnaker.q.Queue
import java.time.Clock

class DynomiteDeadMessageHandler(
  deadLetterQueueName: String,
  private val dynoJedisClient: DynoJedisClient,
  private val mapper: ObjectMapper,
  private val clock: Clock
) : DeadMessageCallback {

  private val dlqKey = "$deadLetterQueueName.messages"

  override fun invoke(queue: Queue, message: Message) {
    dynoJedisClient.zadd(dlqKey, clock.instant().toEpochMilli().toDouble(), mapper.writeValueAsString(message))
  }
}
