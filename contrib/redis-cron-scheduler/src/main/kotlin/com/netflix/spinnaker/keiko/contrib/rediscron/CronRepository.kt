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
package com.netflix.spinnaker.keiko.contrib.rediscron

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.util.Pool
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime

interface CronRepository {

  fun upsert(cronMessage: Cron)

  fun delete(id: String)

  fun find(id: String): Cron?

  fun findAll(): List<Cron>

  fun recordFire(id: String)

  fun getPreviousFires(id: String): List<LocalDateTime>
}

class RedisCronRepository(
  private val namespace: String = "keiko",
  // TODO rz - RedisClientDelegate
  private val pool: Pool<Jedis>,
  private val mapper: ObjectMapper,
  private val clock: Clock
): CronRepository {

  private val ACTIVE_KEY = "{$namespace:cron}:active"

  private val log = LoggerFactory.getLogger(javaClass)

  override fun upsert(cron: Cron) {
    pool.resource.use { redis -> redis.upsertCron(cron) }
  }

  override fun delete(id: String) {
    pool.resource.use { redis ->
      redis.srem(ACTIVE_KEY, id)
      redis.expire(cronKey(id), Duration.ofDays(3).toSeconds())
      redis.expire(cronHistoryKey(id), Duration.ofDays(3).toSeconds())
    }
  }

  override fun find(id: String): Cron? =
    pool.resource.use { redis ->
      redis.get(cronKey(id))
        ?.let { mapper.readValue(it, Wrapper::class.java).cron }
        ?: checkExpiry(redis, id); null
    }

  private fun checkExpiry(redis: Jedis, id: String) {
    val expiry = redis.ttl(cronKey(id))
    if (expiry <= 0) {
      log.debug("Requested cron $id was deleted as is set to cleanup in $expiry seconds")
    }
  }

  override fun findAll(): List<Cron> =
    pool.resource.use { redis ->
      redis.smembers(ACTIVE_KEY)
        .map { mapper.readValue<Wrapper>(it, Wrapper::class.java).cron }
    }

  override fun recordFire(id: String) {
    pool.resource.use { redis ->
      clock.instant().toEpochMilli().also {
        redis.zadd(cronHistoryKey(id), it.toDouble(), it.toString())
      }
    }
  }

  override fun getPreviousFires(id: String): List<LocalDateTime> =
    pool.resource.use { redis ->
      redis.zrangeByScore(cronHistoryKey(id), "-inf", "+inf")
        .map { LocalDateTime.from(Instant.ofEpochMilli(it.toLong())) }
    }

  private fun cronKey(id: String) = "{$namespace:cron}:action:$id"

  private fun cronHistoryKey(id: String) = "{$namespace:cron}:history:$id"

  private fun Jedis.upsertCron(cron: Cron) =
    cron
      .let { mapper.writeValueAsString(Wrapper(it, clock.millis())) }
      .also {
        set(cronKey(cron.id), it)
        sadd(ACTIVE_KEY, cron.id)
      }

  private fun Duration.toSeconds() =
    (this.toMillis() / 1000).toInt()

  private data class Wrapper(
    val cron: Cron,
    val lastModifiedEpochMillis: Long
  )
}
