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

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.CreateQueueRequest
import java.time.temporal.ChronoUnit.SECONDS
import java.time.temporal.TemporalAmount

/**
 * Offers a functional interface for creating SQS queues.
 */
interface AmazonSqsQueueFactory {
  fun invoke(command: CreateQueueCommand): String
}

/**
 * TODO rz - Need to add configuration for a policy before appsec will likely let this be a thing.
 */
class DefaultAmazonSqsQueueFactory(
  private val amazonSqs: AmazonSQS
) : AmazonSqsQueueFactory {

  override fun invoke(command: CreateQueueCommand): String {
    // TODO rz - Setup DLQ as well
    val request = CreateQueueRequest(command.queueName)
      .withAttributes(mapOf(
        "MessageRetentionPeriod" to command.retentionPeriod.get(SECONDS).toString(),
        "ReceiveMessageWaitTimeSeconds" to "20"
      ))
    return amazonSqs.createQueue(request).queueUrl
  }
}

data class CreateQueueCommand(
  val queueName: String,
  val ackTimeout: TemporalAmount,
  val retentionPeriod: TemporalAmount
)
