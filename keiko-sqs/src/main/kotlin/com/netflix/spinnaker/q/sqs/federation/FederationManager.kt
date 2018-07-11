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
package com.netflix.spinnaker.q.sqs.federation

import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.q.DeadMessageCallback
import com.netflix.spinnaker.q.Message
import com.netflix.spinnaker.q.metrics.EventPublisher
import com.netflix.spinnaker.q.sqs.AmazonSqsQueueFactory
import com.netflix.spinnaker.q.sqs.MessageRepository
import com.netflix.spinnaker.q.sqs.SqsQueue
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.Clock
import java.time.temporal.TemporalAmount

/**
 * Handles creation of internal SqsQueue implementations for use by [FederatedSqsQueue].
 */
class FederationManager(
  private val amazonSQSClientFactory: AmazonSqsClientFactory,
  private val amazonSqsQueueFactory: AmazonSqsQueueFactory,
  private val queueFactoryProperties: QueueFactoryProperties,
  private val messageRepository: MessageRepository,
  private val messageFederationStrategy: MessageFederationStrategy,
  private val readFederationStrategy: ReadFederationStrategy,
  private val properties: FederationProperties
) {

  private val log = LoggerFactory.getLogger(javaClass)

  private val queueNames = mutableSetOf<String>()
  private val queues = mutableMapOf<String, SqsQueue>()

  @Scheduled(
    fixedDelayString = "\${queue.sqs.federationRefresh.frequencyMs:60000}",
    initialDelay = 0
  )
  fun refresh() {
    log.info("Refreshing federated queues (partitions: ${properties.numPartitions})")

    0.until(properties.numPartitions)
      .map { "${properties.queueName}.$it.fifo" }
      .forEach { queueName ->
        queues.computeIfAbsent(queueName) {
          log.info("Creating a new SqsQueue: $queueName")
          SqsQueue(
            amazonSQSClientFactory(queueName),
            queueFactoryProperties.amazonCloudWatch,
            amazonSqsQueueFactory,
            messageRepository,
            queueName,
            queueFactoryProperties.clock,
            queueFactoryProperties.mapper,
            queueFactoryProperties.eventPublisher,
            queueFactoryProperties.ackTimeout,
            queueFactoryProperties.deadMessageHandlers
          )
        }
        queueNames.add(queueName)
      }
  }

  fun partition(): String = readFederationStrategy(queueNames.toSet())

  fun partition(message: Message): String = messageFederationStrategy(queueNames.toSet(), message)

  fun queues(): Map<String, SqsQueue> = queues.toMap()
}

data class FederationProperties(
  val queueName: String,
  val numPartitions: Int
)

data class QueueFactoryProperties(
  val amazonCloudWatch: AmazonCloudWatch,
  val clock: Clock,
  val mapper: ObjectMapper,
  val eventPublisher: EventPublisher,
  val ackTimeout: TemporalAmount,
  val deadMessageHandlers: List<DeadMessageCallback>
)
