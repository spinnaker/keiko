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

import com.netflix.spinnaker.q.sqs.randomElement
import java.security.SecureRandom

/**
 * Provided a set of [queueNames], a queue name will be selected for reads.
 */
typealias ReadFederationStrategy = (queueNames: Set<String>) -> String

class RandomReadFederationStrategy : ReadFederationStrategy {

  private val rand = SecureRandom()
  override fun invoke(p1: Set<String>): String = randomElement(rand, p1)
}
