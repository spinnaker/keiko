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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.google.common.hash.Hashing
import com.netflix.spinnaker.q.Message
import java.nio.charset.StandardCharsets
import java.security.SecureRandom

typealias SqsMessage = com.amazonaws.services.sqs.model.Message

internal fun randomElement(rand: SecureRandom, options: Set<String>) =
  rand.nextInt(options.size - 1).let {
    options.elementAt(it)
  }

// Internal ObjectMapper that enforces deterministic property ordering for use only in hashing.
internal val hashObjectMapper = ObjectMapper().copy().apply {
  enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
}

internal fun Message.hash() =
  hashObjectMapper.convertValue(this, MutableMap::class.java)
    .apply { remove("attributes") }
    .let {
      Hashing
        .murmur3_128()
        .hashString(hashObjectMapper.writeValueAsString(it), StandardCharsets.UTF_8)
        .toString()
    }

internal val Message.fingerprint
  get() = hash()
