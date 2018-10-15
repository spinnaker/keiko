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
package com.netflix.spinnaker.config

import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.sqs.AmazonSQS
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.netflix.spinnaker.q.Queue
import com.netflix.spinnaker.q.metrics.EventPublisher
import com.netflix.spinnaker.q.sqs.AmazonSqsQueueFactory
import com.netflix.spinnaker.q.sqs.DefaultAmazonSqsQueueFactory
import com.netflix.spinnaker.q.sqs.DefaultDelayedMessageScheduler
import com.netflix.spinnaker.q.sqs.DelayedMessageScheduler
import com.netflix.spinnaker.q.sqs.LockProvider
import com.netflix.spinnaker.q.sqs.MessageRepository
import com.netflix.spinnaker.q.sqs.SqsQueue
import com.netflix.spinnaker.q.sqs.dynamodb.DynamoDbLockProvider
import com.netflix.spinnaker.q.sqs.dynamodb.DynamoDbMessageRepository
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock
import java.time.Duration
import java.util.Optional

@Configuration
@EnableConfigurationProperties(SqsQueueProperties::class)
class SqsQueueConfiguration {

  @Bean
  @ConditionalOnMissingBean(MessageRepository::class)
  fun messageRepository(
    dynamoDB: AmazonDynamoDB,
    sqsQueueObjectMapper: ObjectMapper,
    sqsQueueProperties: SqsQueueProperties
  ) = DynamoDbMessageRepository(dynamoDB, sqsQueueObjectMapper, sqsQueueProperties.queueName)

  @Bean
  @ConditionalOnMissingBean(AmazonSqsQueueFactory::class)
  fun sqsQueueFactory(amazonSQS: AmazonSQS) = DefaultAmazonSqsQueueFactory(amazonSQS)

  @Bean
  @ConditionalOnMissingBean(name = ["queue"])
  fun queue(
    amazonSQS: AmazonSQS,
    amazonCloudWatch: AmazonCloudWatch,
    amazonSqsQueueFactory: AmazonSqsQueueFactory,
    messageRepository: MessageRepository,
    clock: Clock,
    publisher: EventPublisher,
    sqsQueueObjectMapper: ObjectMapper,
    sqsQueueProperties: SqsQueueProperties
  ) = SqsQueue(
    queueName = sqsQueueProperties.queueName,
    amazonSQS = amazonSQS,
    amazonCloudWatch = amazonCloudWatch,
    amazonSqsQueueFactory = amazonSqsQueueFactory,
    messageRepository = messageRepository,
    clock = clock,
    publisher = publisher,
    mapper =sqsQueueObjectMapper,
    deadMessageHandlers = listOf(),
    ackTimeout = Duration.ofSeconds(sqsQueueProperties.ackTimeoutSeconds.toLong())
  )

  @Bean
  @ConditionalOnMissingBean(LockProvider::class)
  fun lockProvider(
    dynamoDB: AmazonDynamoDB,
    sqsQueueProperties: SqsQueueProperties
  ) = DynamoDbLockProvider(dynamoDB, "${sqsQueueProperties.queueName}_locks")

  @Bean
  @ConditionalOnMissingBean(DelayedMessageScheduler::class)
  fun delayedMessageScheduler(
    lockProvider: LockProvider,
    messageRepository: MessageRepository,
    queue: Queue,
    sqsQueueProperties: SqsQueueProperties
  ) = DefaultDelayedMessageScheduler(
    lockProvider,
    messageRepository,
    queue,
    Duration.ofSeconds(sqsQueueProperties.delayedMessageScheduler.maxDeliveryDelaySeconds.toLong())
  )

  @Bean
  fun sqsQueueObjectMapper(properties: Optional<ObjectMapperSubtypeProperties>): ObjectMapper =
    ObjectMapper().apply {
      registerModule(KotlinModule())
      disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

      SpringObjectMapperConfigurer(
        properties.orElse(ObjectMapperSubtypeProperties())
      ).registerSubtypes(this)
    }
}
