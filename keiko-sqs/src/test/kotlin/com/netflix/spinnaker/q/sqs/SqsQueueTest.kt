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

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.netflix.spinnaker.q.Activator
import com.netflix.spinnaker.q.Message
import com.netflix.spinnaker.q.MessageHandler
import com.netflix.spinnaker.q.Queue
import com.netflix.spinnaker.q.QueueExecutor
import com.netflix.spinnaker.q.QueueProcessor
import com.netflix.spinnaker.q.TestMessage
import com.netflix.spinnaker.q.metrics.EventPublisher
import com.netflix.spinnaker.q.metrics.QueueEvent
import com.netflix.spinnaker.q.sqs.dynamodb.DynamoDbMessageRepository
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executor
import java.util.concurrent.Executors

internal object SqsQueueTest : Spek({

  val queueName = "robztest"

  // Aw yeah casserole.
  val credentialsProvider = EnvironmentVariableCredentialsProvider()

  describe("sqs queue action") {
    val amazonSqs = AmazonSQSClientBuilder.standard().apply {
      region = "us-west-2"
      credentials = credentialsProvider
    }.build()
    val amazonCloudWatch = AmazonCloudWatchClientBuilder.standard().apply {
      region = "us-west-2"
      credentials = credentialsProvider
    }.build()
    val dynamodbClient = AmazonDynamoDBClientBuilder.standard().apply {
      region = "us-west-2"
      credentials = credentialsProvider
    }.build()

    val mapper = ObjectMapper().apply {
      registerKotlinModule()
      registerSubtypes(TestMessage::class.java)
    }

    val queue = SqsQueue(
      amazonSQS = amazonSqs,
      amazonCloudWatch = amazonCloudWatch,
      amazonSqsQueueFactory = DefaultAmazonSqsQueueFactory(amazonSqs),
      messageRepository = DynamoDbMessageRepository(dynamodbClient, mapper, queueName),
      queueName = queueName,
      clock = Clock.systemDefaultZone(),
      mapper = mapper,
      publisher = NoopEventPublisher(),
      ackTimeout = Duration.ofSeconds(30),
      deadMessageHandlers = listOf(deadMessageHandler)
    )


    val processor = QueueProcessor(
      queue = queue,
      executor = BlockingQueueExecutor(),
      handlers = listOf(
        TestMessageHandler(queue)
      ),
      activators = listOf(EnabledActivator(true)),
      deadMessageHandler = deadMessageHandler,
      publisher = NoopEventPublisher()
    )

    while (true) {
      queue.push(TestMessage(Instant.now().toString()))
      queue.push(TestMessage("delayed: ${Instant.now()}"), Duration.ofMinutes(20))

      val state = queue.readState()
      processor.poll()
      Thread.sleep(5000)
    }
  }

})

class NoopEventPublisher : EventPublisher {
  override fun publishEvent(event: QueueEvent) {
  }
}

val deadMessageHandler = { queue: Queue, message: Message ->
  // Do nothing because reasons
}

class BlockingThreadExecutor : Executor {

  private val delegate = Executors.newSingleThreadExecutor()

  override fun execute(command: Runnable) {
    val latch = CountDownLatch(1)
    delegate.execute {
      try {
        command.run()
      } finally {
        latch.countDown()
      }
    }
    latch.await()
  }
}

class BlockingQueueExecutor : QueueExecutor<Executor>(BlockingThreadExecutor()) {
  override fun hasCapacity() = true
  override fun availableCapacity(): Int = 1
}

class TestMessageHandler(override val queue: Queue) : MessageHandler<TestMessage> {
  val log = LoggerFactory.getLogger(javaClass)
  override fun handle(message: TestMessage) {
    log.info(message.toString())
  }

  override val messageType = TestMessage::class.java
}

class EnabledActivator(override val enabled: Boolean = true) : Activator
