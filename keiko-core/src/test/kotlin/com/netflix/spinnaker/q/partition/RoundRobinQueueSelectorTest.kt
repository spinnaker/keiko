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
package com.netflix.spinnaker.q.partition

import com.netflix.spinnaker.q.Queue
import com.netflix.spinnaker.q.SimpleMessage
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.reset
import org.assertj.core.api.Assertions.assertThat
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

object RoundRobinQueueSelectorTest : Spek({

  describe("a round robin queue selector") {

    val queue1: Queue = mock()
    val queue2: Queue = mock()

    fun resetMocks() = reset(
      queue1,
      queue2
    )

    describe("selecting a queue for polling") {
      val subject = RoundRobinQueueSelector(listOf(queue1, queue2))

      afterGroup(::resetMocks)

      on("the first polling cycle") {
        val result = subject.forPoll()

        it("it returns the first queue") {
          assertThat(result).isEqualTo(queue1)
        }
      }

      on("the second polling cycle") {
        val result = subject.forPoll()

        it("returns the second queue") {
          assertThat(result).isEqualTo(queue2)
        }
      }

      on("the third polling cycle") {
        val result = subject.forPoll()

        it("returns the first queue again") {
          assertThat(result).isEqualTo(queue1)
        }
      }
    }

    describe("default partition provider") {
      val subject = RoundRobinQueueSelector(listOf(queue1, queue2))

      afterGroup(::resetMocks)

      given("a message without partition attribute") {
        val results = mutableListOf<Queue>()
        val message = SimpleMessage("hello")

        on("multiple select invocations") {
          (0..9).forEach { results.add(subject.forMessage(message)) }

          it("returns the same queue each time") {
            assertThat(results.toSet()).hasSize(1)
          }
        }
      }

      given("a message with a partition attribute") {
        val results = mutableListOf<Queue>()
        val message = SimpleMessage("hello").apply {
          setAttribute(PartitionAttribute("world"))
        }

        on("multiple select invocations") {
          (0..9).forEach { results.add(subject.forMessage(message)) }

          it("returns the same queue each time") {
            assertThat(results.toSet()).hasSize(1)
          }
        }
      }

      given("two messages with different partitions") {
        val message1 = SimpleMessage("hello").apply {
          setAttribute(PartitionAttribute("1"))
        }
        val message2 = SimpleMessage("hello").apply {
          setAttribute(PartitionAttribute("2"))
        }

        on("select for both messages") {
          val message1Queue = subject.forMessage(message1)
          val message2Queue = subject.forMessage(message2)

          it("returns different queues") {
            assertThat(message1Queue).isNotEqualTo(message2Queue)
          }
        }
      }
    }

    describe("a custom partition provider") {
      val subject = RoundRobinQueueSelector(
        listOf(queue1, queue2),
        fallbackPartitionProvider = { 1 }
      )

      afterGroup(::resetMocks)

      given("a message without partition attribute") {
        val results = mutableListOf<Queue>()
        val message = SimpleMessage("hello")

        on("multiple select invocations") {
          (0..9).forEach { results.add(subject.forMessage(message)) }

          it("returns the same queue each time") {
            assertThat(results.toSet()).hasSize(1)
          }
        }
      }

      given("a message with a partition attribute") {
        val results = mutableListOf<Queue>()
        val message = SimpleMessage("hello").apply {
          setAttribute(PartitionAttribute("world"))
        }

        on("multiple select invocations") {
          (0..9).forEach { results.add(subject.forMessage(message)) }

          it("returns the same queue each time") {
            assertThat(results.toSet()).hasSize(1)
          }
        }
      }

      given("two messages with different partitions") {
        val message1 = SimpleMessage("hello").apply {
          setAttribute(PartitionAttribute("1"))
        }
        val message2 = SimpleMessage("hello").apply {
          setAttribute(PartitionAttribute("2"))
        }

        on("select for both messages") {
          val message1Queue = subject.forMessage(message1)
          val message2Queue = subject.forMessage(message2)

          it("returns the different queues") {
            assertThat(message1Queue).isNotEqualTo(message2Queue)
          }
        }
      }

      given("two messages without partition attributes") {
        val message1 = SimpleMessage("hello")
        val message2 = SimpleMessage("world")

        on("select for both messages") {
          val message1Queue = subject.forMessage(message1)
          val message2Queue = subject.forMessage(message2)

          it("returns the different queues") {
            assertThat(message1Queue).isEqualTo(message2Queue)
          }
        }
      }
    }
  }
})
