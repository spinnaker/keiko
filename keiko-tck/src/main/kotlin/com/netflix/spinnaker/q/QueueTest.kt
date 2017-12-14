/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.q

import com.netflix.spinnaker.spek.and
import com.netflix.spinnaker.time.MutableClock
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.reset
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.verifyZeroInteractions
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.io.Closeable
import java.time.Clock
import java.time.Duration

abstract class QueueTest<out Q : Queue>(
  createQueue: (Clock, DeadMessageCallback) -> Q,
  shutdownCallback: (() -> Unit)? = null
) : Spek({

  var queue: Q? = null
  val callback: QueueCallback = mock()
  val deadLetterCallback: DeadMessageCallback = mock()
  val clock = MutableClock()

  fun resetMocks() = reset(callback, deadLetterCallback)

  fun stopQueue() {
    queue?.let { q ->
      if (q is Closeable) {
        q.close()
      }
    }
    shutdownCallback?.invoke()
  }

  describe("polling the queue") {
    given("there are no messages") {
      beforeGroup {
        queue = createQueue(clock, deadLetterCallback)
      }

      afterGroup(::stopQueue)
      afterGroup(::resetMocks)

      on("polling the queue") {
        queue!!.poll(callback)
      }

      it("does not invoke the callback") {
        verifyZeroInteractions(callback)
      }
    }

    given("there is a single message") {
      val message = TestMessage("a")

      beforeGroup {
        queue = createQueue(clock, deadLetterCallback)
        queue!!.push(message)
      }

      afterGroup(::stopQueue)
      afterGroup(::resetMocks)

      on("polling the queue") {
        queue!!.poll(callback)
      }

      it("passes the queued message to the callback") {
        verify(callback).invoke(eq(message), any())
      }
    }

    given("there are multiple messages") {
      val message1 = TestMessage("a")
      val message2 = TestMessage("b")

      beforeGroup {
        queue = createQueue(clock, deadLetterCallback).apply {
          push(message1)
          clock.incrementBy(Duration.ofSeconds(1))
          push(message2)
        }
      }

      afterGroup(::stopQueue)
      afterGroup(::resetMocks)

      on("polling the queue twice") {
        with(queue!!) {
          poll(callback)
          poll(callback)
        }
      }

      it("passes the messages to the callback in the order they were queued") {
        verify(callback).invoke(eq(message1), any())
        verify(callback).invoke(eq(message2), any())
      }
    }

    given("there is a delayed message") {
      val delay = Duration.ofHours(1)

      and("its delay has not expired") {
        val message = TestMessage("a")

        beforeGroup {
          queue = createQueue(clock, deadLetterCallback)
          queue!!.push(message, delay)
        }

        afterGroup(::stopQueue)
        afterGroup(::resetMocks)

        on("polling the queue") {
          queue!!.poll(callback)
        }

        it("does not invoke the callback") {
          verifyZeroInteractions(callback)
        }
      }

      and("its delay has expired") {
        val message = TestMessage("a")

        beforeGroup {
          queue = createQueue(clock, deadLetterCallback)
          queue!!.push(message, delay)
          clock.incrementBy(delay)
        }

        afterGroup(::stopQueue)
        afterGroup(::resetMocks)

        on("polling the queue") {
          queue!!.poll(callback)
        }

        it("passes the message to the callback") {
          verify(callback).invoke(eq(message), any())
        }
      }
    }
  }
})
