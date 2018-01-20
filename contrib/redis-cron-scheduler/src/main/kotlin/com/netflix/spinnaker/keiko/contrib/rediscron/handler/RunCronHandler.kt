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

import com.netflix.spinnaker.keiko.contrib.rediscron.CronAction
import com.netflix.spinnaker.keiko.contrib.rediscron.CronRepository
import com.netflix.spinnaker.keiko.contrib.rediscron.RunCron
import com.netflix.spinnaker.q.MessageHandler
import com.netflix.spinnaker.q.Queue
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class RunCronHandler(
  override val queue: Queue,
  private val cronRepository: CronRepository,
  private val cronAction: List<CronAction>
) : MessageHandler<RunCron> {

  private val log = LoggerFactory.getLogger(javaClass)

  override val messageType = RunCron::class.java

  override fun handle(message: RunCron) {
    val cron = cronRepository.find(message.id)
    if (cron == null) {
      log.warn("Could not find previously scheduled cron: ${message.id}")
      return
    }

    val action = cronAction.filter { it.javaClass.simpleName == cron.action }
    if (action.size > 1) {
      // TODO rz - should fire an event here instead
      log.error("More than one action for cron was found: $action")
      return
    }

    action.firstOrNull()
      ?.run(cron)
      ?: log.warn("No action found for cron: $cron")
  }
}
