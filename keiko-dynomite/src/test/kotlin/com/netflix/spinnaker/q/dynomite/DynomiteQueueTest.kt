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

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.netflix.dyno.connectionpool.Host
import com.netflix.dyno.connectionpool.HostSupplier
import com.netflix.dyno.connectionpool.TokenMapSupplier
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl
import com.netflix.dyno.connectionpool.impl.lb.HostToken
import com.netflix.dyno.jedis.DynoJedisClient
import com.netflix.dyno.queues.redis.RedisQueues
import com.netflix.dyno.queues.redis.SingleShardSupplier
import com.netflix.spinnaker.kork.jedis.EmbeddedRedis
import com.netflix.spinnaker.q.DeadMessageCallback
import com.netflix.spinnaker.q.QueueTest
import com.netflix.spinnaker.q.metrics.EventPublisher
import com.netflix.spinnaker.q.metrics.QueueEvent
import java.time.Clock
import java.time.Duration

object DynomiteQueueTest : QueueTest<DynomiteQueue>(createQueue, ::shutdownCallback)

private var redis: EmbeddedRedis? = null

private var dynoJedisClient: DynoJedisClient? = null

private var redisQueues: RedisQueues? = null

private val createQueue = { clock: Clock, deadLetterCallback: DeadMessageCallback ->
  redis = EmbeddedRedis.embed()

  dynoJedisClient = createDynoJedisClient(redis!!.port)

  redisQueues = RedisQueues(
    clock,
    dynoJedisClient,
    dynoJedisClient,
    "keiko",
    SingleShardSupplier("queue"),
    30_000,
    1000
  )

  DynomiteQueue(
    redisQueues = redisQueues!!,
    mapper = ObjectMapper()
      .registerModule(KotlinModule())
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES),
    deadMessageHandler = deadLetterCallback,
    publisher = (object : EventPublisher {
      override fun publishEvent(event: QueueEvent) {}
    }),
    ackTimeout = Duration.ofMinutes(1)
  )
}

private fun createDynoJedisClient(port: Int): DynoJedisClient {
  val host = Host("localhost", "127.0.0.1", port, "localrack", "localrac", Host.Status.Up, "{}")
  val hostToken = HostToken(1L, host)

  return DynoJedisClient.Builder()
    .withApplicationName("keiko")
    .withDynomiteClusterName("keiko")
    .withHostSupplier((HostSupplier {
      // TODO rz - connect to local redis instance
      mutableListOf(host)
    }))
    .withCPConfig(
      ConnectionPoolConfigurationImpl("keiko")
        .withTokenSupplier((object : TokenMapSupplier {
          override fun getTokenForHost(host: Host?, activeHosts: MutableSet<Host>?) = hostToken
          override fun getTokens(activeHosts: MutableSet<Host>?) = mutableListOf(hostToken)
        }))
        .setLocalDataCenter("localrac")
        .setLocalRack("localrack")
    )
    .build()
}

private fun shutdownCallback() {
  redis?.destroy()
}
