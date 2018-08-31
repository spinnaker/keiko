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

import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.model.Dimension
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.DeleteMessageRequest
import com.amazonaws.services.sqs.model.MessageAttributeValue
import com.amazonaws.services.sqs.model.ReceiveMessageRequest
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.q.DeadMessageCallback
import com.netflix.spinnaker.q.Message
import com.netflix.spinnaker.q.QueueCallback
import com.netflix.spinnaker.q.metrics.EventPublisher
import com.netflix.spinnaker.q.metrics.MessageProcessing
import com.netflix.spinnaker.q.metrics.MonitorableQueue
import com.netflix.spinnaker.q.metrics.QueueState
import com.netflix.spinnaker.q.metrics.fire
import org.funktionale.partials.partially1
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAmount
import java.util.Date
import java.util.Queue
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

internal const val DELIVERY_TIME = "DeliveryTime"
internal const val SENT_TIMESTAMP = "SentTimestamp"
internal const val FINGERPRINT = "Fingerprint"
internal const val SQS_MAX_DELAY_MINUTES = 15L

class SqsQueue(
  private val amazonSQS: AmazonSQS,
  private val amazonCloudWatch: AmazonCloudWatch,
  amazonSqsQueueFactory: AmazonSqsQueueFactory,
  private val messageRepository: MessageRepository,
  private val queueName: String,
  private val clock: Clock,
  private val mapper: ObjectMapper,
  override val publisher: EventPublisher,
  override val ackTimeout: TemporalAmount,
  override val deadMessageHandlers: List<DeadMessageCallback>
) : MonitorableQueue {

  private val log: Logger = LoggerFactory.getLogger(javaClass)

  private val queueUrl: String
  private val prefetchedMessages = ConcurrentLinkedQueue<MessagePair>()

  init {
    log.info("Configuring queue: $queueName")
    queueUrl = amazonSqsQueueFactory(CreateQueueCommand(
      queueName = queueName,
      ackTimeout = ackTimeout,
      retentionPeriod = Duration.ofHours(6)
    ))
  }

  /**
   * Prefetch messages so that most poll operations don't need to wait to poll.
   */
  private fun prefetchMessages(onDemand: Boolean = false): Queue<MessagePair> {
    if (prefetchedMessages.isEmpty()) {
      if (onDemand) {
        // TODO rz - Would be nice to just record this metric via spectator
        log.debug("Performing prefetch on-demand")
      }

      val invalidMessages = mutableListOf<SqsMessage>()

      val request = ReceiveMessageRequest(queueUrl)
        .withMaxNumberOfMessages(10)
        .withAttributeNames(SENT_TIMESTAMP)
        .withVisibilityTimeout(ackTimeout.get(ChronoUnit.SECONDS).toInt())
        .withReceiveRequestAttemptId(UUID.randomUUID().toString())

      val messages = amazonSQS.receiveMessage(request).messages
        .filterInvalid(invalidMessages)
        .let { validMessages ->
          messageRepository.tryAcquireLocks(validMessages.fingerprints(), ackTimeout)
            .let { acquiredLocks ->
              validMessages.filter { acquiredLocks.contains(it.fingerprint()) }
            }
            .map { Pair(mapper.readValue<Message>(it.body), it) }
        }

      invalidMessages.forEach { removeMessage(it) }

      prefetchedMessages.addAll(messages)
    }
    return prefetchedMessages
  }

  override fun poll(callback: QueueCallback) {
    prefetchMessages(true).firstOrNull()?.let { (message, sqsMessage) ->
      val ack = this::removeMessage.partially1(sqsMessage)

      val deliveryTime = (sqsMessage.attributes[DELIVERY_TIME] ?: sqsMessage.attributes[SENT_TIMESTAMP])
        ?.toLongOrNull()
        ?.let { Instant.ofEpochMilli(it) }
        ?: Instant.MIN

      fire(MessageProcessing(message, deliveryTime, clock.instant()))
      callback(message, ack)
    }
  }

  override fun push(message: Message, delay: TemporalAmount) {
    if (Duration.from(delay) > Duration.ofMinutes(SQS_MAX_DELAY_MINUTES)) {
      messageRepository.schedule(message, delay)
    } else {
      val request = SendMessageRequest(queueUrl, mapper.writeValueAsString(message)).apply {
        addAttribute(FINGERPRINT, message.fingerprint, "String")
        if (Duration.from(delay) > Duration.ZERO) {
          addAttribute(DELIVERY_TIME, Instant.now().plus(delay).toEpochMilli().toString(), "Number")
        }
      }
      amazonSQS.sendMessage(request)
    }
  }

  override fun reschedule(message: Message, delay: TemporalAmount) {
    messageRepository.schedule(message, delay)
  }

  override fun ensure(message: Message, delay: TemporalAmount) {
    // Do we track all messages inbound and acked? Perhaps it's better to just kill this method off
    // entirely and have anything that implements it use [reschedule] instead? Not sure anything
    // actually does use this now that Keel has been rewritten.
    throw NotImplementedError("ensure is not supported by SQS")
  }

  override fun containsMessage(predicate: (Message) -> Boolean): Boolean {
    // TODO - rz Need to change the interface to not require this. This interface is so crazy.
    return false
  }

  // TODO rz - Move this logic to a separate class, I think?
  override fun readState(): QueueState {
    val metricNames = listOf(
      "ApproximateNumberOfMessagesNotVisible",
      "ApproximateNumberOfMessagesVisible",
      "ApproximateNumberOfMessagesDelayed"
    )

    val metrics = metricNames
      .mapNotNull { metric ->
        val request = GetMetricStatisticsRequest().apply {
          namespace = "AWS/SQS"
          metricName = metric
          period = 60
          withDimensions(listOf(
            Dimension().apply {
              name = "QueueName"
              value = queueName
            }
          ))
          startTime = Date.from(Instant.now().minus(Duration.ofSeconds(60)))
          endTime = Date.from(Instant.now())
          withStatistics(listOf("Maximum"))
        }

        amazonCloudWatch.getMetricStatistics(request).datapoints.firstOrNull()
          ?.let { metric to it.maximum.toInt() }
      }
      .toMap()

    val notVisible = metrics["ApproximateNumberOfMessagesNotVisible"] ?: 0
    val visible = metrics["ApproximateNumberOfMessagesVisible"] ?: 0
    val delayed = metrics["ApproximateNumberOfMessagesDelayed"] ?: 0

    return QueueState(
      depth = notVisible + visible + delayed,
      ready = visible,
      unacked = notVisible
    )
  }

  private fun removeMessage(message: SqsMessage) {
    amazonSQS.deleteMessage(DeleteMessageRequest(queueUrl, message.receiptHandle))
    prefetchedMessages.removeIf { it.second == message }
  }

  private fun List<SqsMessage>.filterInvalid(invalidMessages: MutableList<SqsMessage>): List<SqsMessage> {
    val list = mutableListOf<SqsMessage>()
    for (element in this) {
      val fingerprint = element.attributes[FINGERPRINT]
      if (fingerprint == null) {
        log.warn("Received invalid message (no fingerprint), deleting: ${element.messageId}")
        invalidMessages.add(element)
        continue
      }
      if (element.body == null) {
        log.warn("Received invalid message (null body), deleting: $fingerprint")
        invalidMessages.add(element)
        continue
      }
      list.add(element)
    }
    return list
  }

  private fun SendMessageRequest.addAttribute(key: String, value: String, dataType: String) {
    addMessageAttributesEntry(
      key,
      MessageAttributeValue()
        .withDataType(dataType)
        .withStringValue(value)
    )
  }

  private fun List<SqsMessage>.fingerprints() =
    mapNotNull { it.attributes[FINGERPRINT] }

  private fun SqsMessage.fingerprint() =
    attributes[FINGERPRINT] ?: throw SqsFingerprintAttributeUndefined(messageId)

  private class SqsFingerprintAttributeUndefined(messageId: String)
    : IllegalStateException("Fingerprint must have already been set on SQS message: $messageId")
}

private typealias MessagePair = Pair<Message, SqsMessage>
