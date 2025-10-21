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

import java.nio.file.Path;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Configuration for ObjectNodePublisher with optional spillover support.
 *
 * <p><strong>DEFAULT BEHAVIOR:</strong> Spillover is <strong>DISABLED</strong> unless explicitly
 * configured. All existing ObjectNodePublisher constructors use default configuration with
 * spillover disabled, ensuring 100% backward compatibility.
 *
 * <p><strong>Spillover:</strong> When enabled, automatically spills the remaining input stream to
 * a temporary file if the subscriber consumption rate drops below the configured threshold. This
 * prevents blocking slow network/HTTP streams for extended periods.
 *
 * <p><strong>Example - Spillover DISABLED (default):</strong>
 * <pre>{@code
 * // These constructors use default config - NO spillover
 * new ObjectNodePublisher(inputStream)
 * new ObjectNodePublisher(inputStream, retryStream)
 * new PublisherConfig()  // minEventsPerSecond = -1 (disabled)
 * }</pre>
 *
 * <p><strong>Example - Spillover ENABLED:</strong>
 * <pre>{@code
 * // Explicit threshold - enable spillover
 * new ObjectNodePublisher(inputStream, retryStream, 1.0)  // Spillover if < 1 event/sec
 *
 * // Or via config builder
 * PublisherConfig config = PublisherConfig.builder()
 *     .minEventsPerSecond(0.5)    // Enable spillover if < 0.5 events/sec
 *     .spilloverCheckDelay(10000) // Check after 10 seconds
 *     .spillParsedEvents(false)   // Don't spill parsed events
 *     .build();
 * new ObjectNodePublisher(inputStream, config);
 * }</pre>
 */
@Getter
@Builder
@AllArgsConstructor
public class PublisherConfig {
  /**
   * Minimum events per second threshold for spillover activation.
   *
   * <p><strong>Default: -1 (DISABLED)</strong>
   *
   * <p>When set to -1 (default), spillover is completely disabled and has zero performance
   * overhead. This ensures backward compatibility with existing code.
   *
   * <p>When set to >= 0, spillover is enabled. If the subscriber consumption rate drops below this
   * threshold, the remaining input stream will be spilled to a temporary file.
   *
   * <p>Examples:
   * <ul>
   *   <li>-1.0 = Disabled (default, no spillover ever)</li>
   *   <li>1.0 = Spillover if consumption rate drops below 1 event per second</li>
   *   <li>0.5 = Spillover if consumption rate drops below 1 event per 2 seconds</li>
   *   <li>0.1 = Spillover if consumption rate drops below 1 event per 10 seconds</li>
   * </ul>
   */
  @Builder.Default
  private final double minEventsPerSecond = -1.0;

  /**
   * Delay in milliseconds before starting to check consumption rate.
   *
   * <p><strong>Default: 5000ms (5 seconds)</strong>
   *
   * <p>The publisher will wait this long after starting before checking the consumption rate. This
   * allows the subscriber time to warm up and avoids false positives during initialization.
   *
   * <p>Only relevant when spillover is enabled (minEventsPerSecond >= 0).
   */
  @Builder.Default
  private final long spilloverCheckDelayMs = 5000;

  /**
   * Whether to spill already-parsed ObjectNode events to disk.
   *
   * <p><strong>Default: false</strong>
   *
   * <p>When false (default), only unparsed JSON bytes from the input stream are spilled to the
   * temporary file. Already-parsed ObjectNode events remain in memory.
   *
   * <p>When true, parsed events are re-serialized and written to the temporary file along with
   * unparsed input. This frees more memory but requires additional serialization overhead.
   *
   * <p>Only relevant when spillover is enabled (minEventsPerSecond >= 0).
   */
  @Builder.Default
  private final boolean spillParsedEvents = false;

  /**
   * Directory for temporary spillover files.
   *
   * <p><strong>Default: null (use system temp directory)</strong>
   *
   * <p>When null (default), temporary files are created in the system temp directory (e.g.,
   * /tmp on Linux/Mac, %TEMP% on Windows).
   *
   * <p>When set to a specific path, temporary files are created in that directory.
   *
   * <p>Only relevant when spillover is enabled (minEventsPerSecond >= 0).
   */
  @Builder.Default
  private final Path tempDirectory = null;

  /**
   * Whether to automatically delete temporary spillover files after processing.
   *
   * <p><strong>Default: true</strong>
   *
   * <p>When true (default), temporary files are automatically deleted after the publisher
   * completes (success or error).
   *
   * <p>When false, temporary files are left on disk (useful for debugging).
   *
   * <p>Only relevant when spillover is enabled (minEventsPerSecond >= 0).
   */
  @Builder.Default
  private final boolean autoDeleteTempFile = true;

  /**
   * Creates a default configuration with spillover DISABLED.
   *
   * <p>This ensures backward compatibility - existing code continues to work unchanged with no
   * spillover behavior.
   *
   * @return default configuration with spillover disabled
   */
  public PublisherConfig() {
    this(-1.0, 5000, false, null, true);
  }

  /**
   * Checks if spillover is enabled.
   *
   * @return true if spillover is enabled (minEventsPerSecond >= 0), false otherwise
   */
  public boolean isSpilloverEnabled() {
    return minEventsPerSecond >= 0;
  }

  /**
   * Validates the configuration.
   *
   * @throws IllegalArgumentException if configuration is invalid
   */
  public void validate() {
    if (minEventsPerSecond < -1.0) {
      throw new IllegalArgumentException(
          "minEventsPerSecond must be >= -1.0 (got: " + minEventsPerSecond + ")");
    }
    if (spilloverCheckDelayMs < 0) {
      throw new IllegalArgumentException(
          "spilloverCheckDelayMs must be >= 0 (got: " + spilloverCheckDelayMs + ")");
    }
  }
}
