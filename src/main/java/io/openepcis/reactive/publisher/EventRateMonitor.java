/*
 * Copyright 2022-2025 benelog GmbH & Co. KG
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */
package io.openepcis.reactive.publisher;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitors event consumption rate to determine when spillover should be triggered.
 *
 * <p>This class tracks the number of events emitted and the time elapsed to calculate the current
 * consumption rate in events per second. It respects a configurable delay period before starting
 * to check the rate, allowing subscribers time to warm up.
 *
 * <p><strong>Thread-safe:</strong> Uses atomic operations for concurrent access.
 *
 * <p>Package-private - internal implementation detail of ObjectNodePublisher.
 */
class EventRateMonitor {
  private final PublisherConfig config;
  private final long startTimeMs;
  private final AtomicLong eventCount = new AtomicLong(0);
  private final AtomicLong firstEventTimeMs = new AtomicLong(-1);

  /**
   * Creates a new event rate monitor.
   *
   * @param config publisher configuration containing spillover settings
   */
  EventRateMonitor(PublisherConfig config) {
    this.config = config;
    this.startTimeMs = System.currentTimeMillis();
  }

  /**
   * Records that an event was emitted.
   *
   * <p>Call this method each time an event is successfully emitted to a subscriber.
   */
  void recordEvent() {
    long count = eventCount.incrementAndGet();

    // Record time of first event for more accurate rate calculation
    if (count == 1) {
      firstEventTimeMs.compareAndSet(-1, System.currentTimeMillis());
    }
  }

  /**
   * Checks if enough time has elapsed to start checking the consumption rate.
   *
   * <p>Returns false during the initial delay period (spilloverCheckDelayMs) to allow the
   * subscriber time to warm up and avoid false positives.
   *
   * @return true if rate checking should begin, false if still in delay period
   */
  boolean shouldCheckRate() {
    long elapsedMs = System.currentTimeMillis() - startTimeMs;
    return elapsedMs >= config.getSpilloverCheckDelayMs();
  }

  /**
   * Calculates the current event consumption rate in events per second.
   *
   * <p>The rate is calculated from the time the first event was emitted (not from publisher
   * creation) for more accurate measurement.
   *
   * @return events per second, or Double.MAX_VALUE if no events emitted yet
   */
  double getCurrentRate() {
    long count = eventCount.get();

    // No events yet - return max rate (don't trigger spillover)
    if (count == 0) {
      return Double.MAX_VALUE;
    }

    long firstEvent = firstEventTimeMs.get();
    if (firstEvent == -1) {
      return Double.MAX_VALUE;
    }

    // Calculate elapsed time since first event
    long elapsedMs = System.currentTimeMillis() - firstEvent;

    // Avoid division by zero
    if (elapsedMs <= 0) {
      return Double.MAX_VALUE;
    }

    // Calculate events per second
    double elapsedSeconds = elapsedMs / 1000.0;
    return count / elapsedSeconds;
  }

  /**
   * Gets the total number of events emitted so far.
   *
   * @return total event count
   */
  long getEventCount() {
    return eventCount.get();
  }

  /**
   * Checks if the current rate is below the configured minimum threshold.
   *
   * <p>Only checks if the rate check delay has elapsed and spillover is enabled.
   *
   * @return true if spillover should be triggered, false otherwise
   */
  boolean isBelowThreshold() {
    // Don't check if spillover is disabled
    if (!config.isSpilloverEnabled()) {
      return false;
    }

    // Don't check if still in delay period
    if (!shouldCheckRate()) {
      return false;
    }

    // Check if current rate is below minimum
    return getCurrentRate() < config.getMinEventsPerSecond();
  }

  /**
   * Gets diagnostic information about the current monitoring state.
   *
   * @return diagnostic string with current rate, event count, and elapsed time
   */
  String getDiagnostics() {
    long count = eventCount.get();
    long elapsedMs = System.currentTimeMillis() - startTimeMs;
    double rate = getCurrentRate();

    return String.format(
        "EventRateMonitor[events=%d, elapsedMs=%d, rate=%.2f events/sec, threshold=%.2f, shouldCheck=%b]",
        count, elapsedMs, rate, config.getMinEventsPerSecond(), shouldCheckRate());
  }
}
