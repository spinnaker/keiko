package com.netflix.spinnaker.q.metrics

class NoopEventPublisher : EventPublisher {
  override fun publishEvent(event: QueueEvent) {}
}
