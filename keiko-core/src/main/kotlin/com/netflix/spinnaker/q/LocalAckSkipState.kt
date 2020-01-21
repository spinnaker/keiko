/*
 * Copyright 2020 Netflix, Inc.
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
package com.netflix.spinnaker.q

/**
 * A utility class that helps avoid a potential concurrency issue in [Queue.ackAndPush].
 *
 * When [Queue.ackAndPush] is called, it's possible that another queue consumer could pick up the
 * message and then have this local process immediately ack it. By using this local thread,
 * implementations of [Queue.ackAndPush] will be able to tell the [QueueProcessor] to skip running
 * the ack callback on the original [Queue.poll] cycle.
 */
class LocalAckSkipState {

  /**
   * Skip the next queue ACK callback.
   */
  fun skipAck() {
    skipAck.set(true)
  }

  fun maybeSkipAck(callback: () -> Unit) {
    try {
      if (!skipAck.get()) {
        callback.invoke()
      }
    } finally {
      skipAck.set(false)
    }
  }

  companion object {
    private val skipAck = ThreadLocal.withInitial { false }
  }
}
