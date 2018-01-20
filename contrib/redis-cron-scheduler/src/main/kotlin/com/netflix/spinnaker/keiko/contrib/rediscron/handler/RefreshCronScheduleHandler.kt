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
package com.netflix.spinnaker.keiko.contrib.rediscron.handler

import com.cronutils.model.CronType.UNIX
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import com.netflix.spinnaker.config.RefreshCronScheduleHandlerProperties
import com.netflix.spinnaker.keiko.contrib.rediscron.Cron
import com.netflix.spinnaker.keiko.contrib.rediscron.CronRepository
import com.netflix.spinnaker.keiko.contrib.rediscron.RefreshCronSchedule
import com.netflix.spinnaker.keiko.contrib.rediscron.RunCron
import com.netflix.spinnaker.q.MessageHandler
import com.netflix.spinnaker.q.Queue
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import java.time.ZonedDateTime
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS

@Component
class RefreshCronScheduleHandler(
  override val queue: Queue,
  private val cronRepository: CronRepository,
  private val clock: Clock,
  properties: RefreshCronScheduleHandlerProperties
) : MessageHandler<RefreshCronSchedule> {

  private val log = LoggerFactory.getLogger(javaClass)

  private val executor = Executors.newScheduledThreadPool(1)

  init {
    // TODO rz - possible race condition; should acquire lock at beginning of handle...
    executor.scheduleWithFixedDelay({
      queue.ensure(RefreshCronSchedule(), Duration.ofSeconds(30))
    }, 0, properties.intervalSeconds, SECONDS)
  }

  override fun handle(message: RefreshCronSchedule) {
    val parser = CronParser(CronDefinitionBuilder.instanceDefinitionFor(UNIX))

    // Find all valid crons and schedule run tasks for their next executions
    cronRepository
      .findAll()
      .mapNotNull { cron ->
        cron.nextExecution(parser).let { nextExecution ->
          if (nextExecution.isPresent) {
            Pair(RunCron(cron.id), nextExecution.get().toEpochSecond() - clock.instant().epochSecond)
          } else {
            null
          }
        }
      }
      .also { log.info("Scheduling ${it.size} cron schedules") }
      .forEach { queue.ensure(it.first, Duration.ofSeconds(it.second)) }
  }

  override val messageType = RefreshCronSchedule::class.java

  private fun Cron.nextExecution(parser: CronParser)
    = ExecutionTime.forCron(parser.parse(expression)).nextExecution(ZonedDateTime.now())

}
