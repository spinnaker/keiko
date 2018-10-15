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
package com.netflix.spinnaker.q.sqs.dynamodb

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.PrimaryKey
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType.HASH
import com.amazonaws.services.dynamodbv2.model.KeyType.RANGE
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.q.Message
import com.netflix.spinnaker.q.sqs.MessageRepository
import com.netflix.spinnaker.q.sqs.fingerprint
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.temporal.TemporalAmount

internal const val MESSAGE_FINGERPRINT = "MessageFingerprint"
internal const val DELIVERY_TIME = "DeliveryTime"
internal const val MESSAGE_BODY = "MessageBody"
internal const val TTL = "Ttl"
internal const val MESSAGE_ID = "MessageID"

class DynamoDbMessageRepository(
  private val client: AmazonDynamoDB,
  private val mapper: ObjectMapper,
  queueName: String
) : MessageRepository {

  private val log = LoggerFactory.getLogger(javaClass)

  private val schedulesTableName = "keiko_${queueName}_schedules"
  private val locksTableName = "keiko_${queueName}_locks"

  private val dynamoDB = DynamoDB(client)
  private val schedulesTable: Table
  private val locksTable: Table

  init {
    // TODO rz - Probably will want pagination here
    val tables = client.listTables().tableNames
    schedulesTable = ensureSchedulesTable(dynamoDB, tables)
    locksTable = ensureLocksTable(dynamoDB, tables)
  }

  override fun tryAcquireLocks(messageIds: List<String>, ttl: TemporalAmount): List<String> {
    if (messageIds.isEmpty()) {
      return listOf()
    }

    return messageIds.mapNotNull { it ->
      try {
        PutItemSpec()
          .withItem(
            Item()
              .withPrimaryKey(MESSAGE_ID, it)
              .withLong(TTL, Instant.now().plus(ttl).toEpochMilli())
          )
          .withConditionExpression("attribute_not_exists($MESSAGE_ID)")
          .also { item ->
            schedulesTable.putItem(item)
          }
        return@mapNotNull it
      } catch (e: ConditionalCheckFailedException) {
        // Ignore, we just didn't get the lock on this message and that's ok.
        return@mapNotNull null
      }
    }
  }

  override fun schedule(message: Message, delay: TemporalAmount) {
    // This will override any existing schedule that exists for this fingerprint
    val deliveryTime = Instant.now().plus(delay)
    Item()
      .withPrimaryKey(
        MESSAGE_FINGERPRINT, message.fingerprint,
        DELIVERY_TIME, deliveryTime.toEpochMilli())
      .withString(MESSAGE_BODY, mapper.writeValueAsString(message))
      // TODO rz - Totally a possibility that we'll have messages scheduled out further than 6hr
      .withLong(TTL, deliveryTime.plus(Duration.ofHours(6)).toEpochMilli())
      .also {
        schedulesTable.putItem(it)
      }
  }

  override fun schedules(fingerprints: List<String>): Map<String, Instant> {
    if (fingerprints.isEmpty()) {
      return mapOf()
    }

    return TableKeysAndAttributes(schedulesTableName)
      .apply {
        fingerprints.forEach {
          addPrimaryKey(PrimaryKey(MESSAGE_FINGERPRINT, it))
        }
        withAttributeNames(DELIVERY_TIME)
      }
      .let { dynamoDB.batchGetItem(it).tableItems }
      .flatMap { it.value }
      .mapNotNull {
        if (!(it.isPresent(MESSAGE_FINGERPRINT) && it.isPresent(DELIVERY_TIME))) {
          log.error("Table has item without $MESSAGE_FINGERPRINT or $DELIVERY_TIME")
          null
        } else {
          it.getString(MESSAGE_FINGERPRINT) to Instant.ofEpochMilli(it.getLong(DELIVERY_TIME))
        }
      }
      .toMap()
  }

  override fun scheduled(maxDeliveryTime: Instant, callback: (Message, Instant) -> Unit) {
    val expressionAttributeValues = mapOf(
      ":maxDeliveryTime" to AttributeValue().withN("${maxDeliveryTime.toEpochMilli()}")
    )

    var lastKeyEvaluated: Map<String, AttributeValue>? = null
    do {
      val request = ScanRequest(schedulesTableName).let {
        it.setAttributesToGet(listOf(MESSAGE_FINGERPRINT, DELIVERY_TIME, MESSAGE_BODY))
        it.withFilterExpression("$DELIVERY_TIME > :maxDeliveryTime")
        it.withExpressionAttributeValues(expressionAttributeValues)
        it.withExclusiveStartKey(lastKeyEvaluated)
        it.withLimit(50)
      }

      val result = client.scan(request)

      result.items.forEach {
        val message = mapper.readValue<Message>(it.getValue(MESSAGE_BODY).s)
        val deliveryTime = Instant.ofEpochMilli(it.getValue(DELIVERY_TIME).n.toLong())
        callback(message, deliveryTime)
      }

      lastKeyEvaluated = result.lastEvaluatedKey
    } while (lastKeyEvaluated != null)
  }

  private fun ensureSchedulesTable(dynamoDB: DynamoDB, tables: List<String>): Table {
    if (!tables.contains(schedulesTableName)) {
      log.info("Table missing, creating: $schedulesTableName")
      createTable(
        schedulesTableName,
        listOf(
          KeySchemaElement(MESSAGE_FINGERPRINT, HASH),
          KeySchemaElement(DELIVERY_TIME, RANGE)
        ),
        listOf(
          AttributeDefinition(MESSAGE_FINGERPRINT, "S"),
          AttributeDefinition(DELIVERY_TIME, "N")
        )
      )
    }
    return dynamoDB.getTable(schedulesTableName)
  }

  private fun ensureLocksTable(dynamoDB: DynamoDB, tables: List<String>): Table {
    if (!tables.contains(locksTableName)) {
      log.info("Table missing, creating: $locksTableName")
      createTable(
        locksTableName,
        listOf(KeySchemaElement(MESSAGE_ID, HASH)),
        listOf(AttributeDefinition(MESSAGE_ID, "S"))
      )
    }
    return dynamoDB.getTable(locksTableName)
  }

  private fun createTable(
    tableName: String,
    keySchema: List<KeySchemaElement>,
    attributeDefinitions: List<AttributeDefinition>
  ) {

    dynamoDB.createTable(
      CreateTableRequest()
        .withTableName(tableName)
        .withKeySchema(keySchema)
        .withAttributeDefinitions(attributeDefinitions)
        // TODO rz - configuration. autoscaling?
        .withProvisionedThroughput(ProvisionedThroughput(5, 5))
    ).run {
      log.info("Waiting for table $tableName to be active")
      waitForActive()

      try {
        client.updateTimeToLive(
          UpdateTimeToLiveRequest()
            .withTableName(tableName)
            .withTimeToLiveSpecification(
              TimeToLiveSpecification()
                .withAttributeName(TTL)
                .withEnabled(true)
            )
        )
      } catch (e: AmazonDynamoDBException) {
        // TEST ONLY: Localstack does not currently support updateTimeToLive
        if (e.errorCode != "UnknownOperationException") {
          throw e
        }
      }
      log.info("Table $tableName is now active")
    }
  }
}
