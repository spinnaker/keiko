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
package com.netflix.spinnaker.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.netflix.discovery.DiscoveryClient
import com.netflix.dyno.connectionpool.Host
import com.netflix.dyno.connectionpool.HostSupplier
import com.netflix.dyno.connectionpool.TokenMapSupplier
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl
import com.netflix.dyno.connectionpool.impl.lb.HostToken
import com.netflix.dyno.jedis.DynoJedisClient
import com.netflix.dyno.queues.redis.RedisQueues
import com.netflix.dyno.queues.redis.SingleShardSupplier
import com.netflix.spinnaker.q.DeadMessageCallback
import com.netflix.spinnaker.q.dynomite.DynomiteDeadMessageHandler
import com.netflix.spinnaker.q.dynomite.DynomiteQueue
import com.netflix.spinnaker.q.metrics.EventPublisher
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock
import java.time.Duration
import java.util.*

@Configuration
@ConditionalOnProperty("keiko.queue.dynomite.enabled")
@EnableConfigurationProperties(DynomiteQueueConfiguration::class)
open class DynomiteQueueConfiguration {

  @Bean
  @ConfigurationProperties("keiko.queue.dynomite.connectionPool")
  open fun connectionPoolConfigurationImpl(dynomiteConfigurationProperties: DynomiteConfigurationProperties): ConnectionPoolConfigurationImpl {
    return ConnectionPoolConfigurationImpl(dynomiteConfigurationProperties.applicationName)
  }

  @Bean(destroyMethod = "stopClient")
  open fun dynoJedisClient(dynomiteConfigurationProperties: DynomiteConfigurationProperties,
                           connectionPoolConfiguration: ConnectionPoolConfigurationImpl,
                           discoveryClient: Optional<DiscoveryClient>): DynoJedisClient
    = createDynoJedisClient(dynomiteConfigurationProperties, connectionPoolConfiguration, discoveryClient)

  @Bean open fun redisQueues(dynomiteConfigurationProperties: DynomiteConfigurationProperties,
                           dynoJedisClient: DynoJedisClient): RedisQueues {
    return RedisQueues(
      dynoJedisClient,
      dynoJedisClient,
      dynomiteConfigurationProperties.keyPrefix,
      // TODO rz - add support for DynoShardSupplier
      SingleShardSupplier(dynomiteConfigurationProperties.queueName),
      Duration.ofSeconds(dynomiteConfigurationProperties.ackTimeoutSeconds.toLong()).toMillis().toInt(),
      Duration.ofSeconds(30).toMillis().toInt()
    )
  }

  @Bean open fun dynomiteQueue(dynomiteConfigurationProperties: DynomiteConfigurationProperties,
                               redisQueues: RedisQueues,
                               redisQueueObjectMapper: ObjectMapper,
                               deadMessageHandler: DeadMessageCallback,
                               publisher: EventPublisher) =
    DynomiteQueue(
      redisQueues = redisQueues,
      mapper = redisQueueObjectMapper,
      deadMessageHandler = deadMessageHandler,
      publisher = publisher,
      ackTimeout = Duration.ofSeconds(dynomiteConfigurationProperties.ackTimeoutSeconds.toLong())
    )

  @Bean open fun dynomiteDeadMessageHandler(
    dynomiteConfigurationProperties: DynomiteConfigurationProperties,
    dynoJedisClient: DynoJedisClient,
    redisQueueObjectMapper: ObjectMapper,
    clock: Clock
  ) =
    DynomiteDeadMessageHandler(
      dynomiteConfigurationProperties.deadLetterQueueName,
      dynoJedisClient,
      redisQueueObjectMapper,
      clock
    )

  @Bean
  @ConditionalOnMissingBean
  open fun redisQueueObjectMapper(): ObjectMapper =
    ObjectMapper()
      .registerModule(KotlinModule())
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

  private fun createDynoJedisClient(dynomiteConfigurationProperties: DynomiteConfigurationProperties,
                                    connectionPoolConfiguration: ConnectionPoolConfigurationImpl,
                                    discoveryClient: Optional<DiscoveryClient>): DynoJedisClient {
    val builder = DynoJedisClient.Builder()
      .withApplicationName(dynomiteConfigurationProperties.applicationName)
      .withDynomiteClusterName(dynomiteConfigurationProperties.clusterName)

    return discoveryClient.map({ dc ->
      builder.withDiscoveryClient(dc)
        .withCPConfig(connectionPoolConfiguration)
    }).orElseGet({
      connectionPoolConfiguration
        .withTokenSupplier( StaticTokenMapSupplier(dynomiteConfigurationProperties.getDynoHostTokens()))
        .setLocalDataCenter(dynomiteConfigurationProperties.localDataCenter)
        .setLocalRack(dynomiteConfigurationProperties.localRack)

      builder
        .withHostSupplier( StaticHostSupplier(dynomiteConfigurationProperties.getDynoHosts()))
        .withCPConfig(connectionPoolConfiguration)
    }).build()
  }

  class StaticHostSupplier(private val hosts: MutableCollection<Host>) : HostSupplier {
    override fun getHosts() = hosts
  }

  class StaticTokenMapSupplier(private val ht: MutableList<HostToken>) : TokenMapSupplier {
    override fun getTokenForHost(host: Host, activeHosts: MutableSet<Host>) = ht.find { it.host == host }!!
    override fun getTokens(activeHosts: MutableSet<Host>): MutableList<HostToken> = ht
  }
}
