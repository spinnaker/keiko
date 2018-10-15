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

import com.amazonaws.services.dynamodbv2.AcquireLockOptions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions
import com.amazonaws.services.dynamodbv2.CreateDynamoDBTableOptions
import com.amazonaws.services.dynamodbv2.model.LockTableDoesNotExistException
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.netflix.spinnaker.q.sqs.LockProvider
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import javax.annotation.PreDestroy

class DynamoDbLockProvider(
  dynamoDB: AmazonDynamoDB,
  tableName: String
) : LockProvider {

  private val log = LoggerFactory.getLogger(javaClass)

  private val client = AmazonDynamoDBLockClient(
    AmazonDynamoDBLockClientOptions.builder(dynamoDB, tableName)
      .withTimeUnit(TimeUnit.SECONDS)
      .withLeaseDuration(10L)
      .withHeartbeatPeriod(3L)
      .withCreateHeartbeatBackgroundThread(true)
      .build()
  )

  init {
    try {
      client.assertLockTableExists()
    } catch (e: LockTableDoesNotExistException) {
      log.info("Creating lock table: $tableName")
      AmazonDynamoDBLockClient.createLockTableInDynamoDB(
        CreateDynamoDBTableOptions.builder(dynamoDB, ProvisionedThroughput(5, 5), tableName)
          .build()
      )
    }
    client.assertLockTableExists()
  }

  @PreDestroy
  fun shutdown() {
    client.close()
  }

  override fun tryAcquire(lockName: String, callback: () -> Unit) {
    log.debug("Attempting to acquire lock: $lockName")
    try {
      val lockItem = client.tryAcquireLock(AcquireLockOptions.builder(lockName).build())
      lockItem.ifPresent {
        callback()
        client.releaseLock(it)
      }
    } catch (e: InterruptedException) {
      log.warn("Lock was interrupted: $lockName", e)
    } catch (e: Exception) {
      log.error("Unexpected error while acquiring or releasing lock: $lockName", e)
    }
  }
}
