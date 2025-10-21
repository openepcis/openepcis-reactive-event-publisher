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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/**
 * Fluent builder for creating ObjectNodePublisher instances with custom configuration.
 *
 * <p>Provides a type-safe, readable way to configure publishers with optional spillover support.
 *
 * <p><strong>Example - Simple usage:</strong>
 * <pre>{@code
 * ObjectNodePublisher<ObjectNode> publisher = ObjectNodePublisher.builder()
 *     .inputStream(myInputStream)
 *     .build();
 * }</pre>
 *
 * <p><strong>Example - With spillover:</strong>
 * <pre>{@code
 * ObjectNodePublisher<ObjectNode> publisher = ObjectNodePublisher.builder()
 *     .inputStream(myInputStream)
 *     .retryInputStream(() -> new FileInputStream("data.json"))
 *     .minEventsPerSecond(1.0)      // Enable spillover
 *     .spilloverCheckDelay(10000)   // Check after 10 seconds
 *     .spillParsedEvents(false)     // Don't spill parsed events
 *     .build();
 * }</pre>
 *
 * @param <T> type of ObjectNode emitted by the publisher
 */
public class ObjectNodePublisherBuilder<T extends ObjectNode> {
  private InputStream inputStream;
  private Reader reader;
  private Callable<InputStream> retryInputStream;
  private Callable<Reader> retryReader;

  // Config builder for spillover settings
  private final PublisherConfig.PublisherConfigBuilder configBuilder = PublisherConfig.builder();

  /**
   * Creates a new builder instance.
   */
  ObjectNodePublisherBuilder() {
  }

  /**
   * Sets the input stream to parse.
   *
   * <p>Either inputStream or reader must be set, but not both.
   *
   * @param inputStream the input stream
   * @return this builder
   */
  public ObjectNodePublisherBuilder<T> inputStream(InputStream inputStream) {
    this.inputStream = inputStream;
    return this;
  }

  /**
   * Sets the reader to parse.
   *
   * <p>Either inputStream or reader must be set, but not both.
   *
   * @param reader the reader
   * @return this builder
   */
  public ObjectNodePublisherBuilder<T> reader(Reader reader) {
    this.reader = reader;
    return this;
  }

  /**
   * Sets the retry input stream callable.
   *
   * <p>Used for retry support when eventList appears early in the document.
   *
   * @param retryInputStream callable that provides a fresh input stream
   * @return this builder
   */
  public ObjectNodePublisherBuilder<T> retryInputStream(Callable<InputStream> retryInputStream) {
    this.retryInputStream = retryInputStream;
    return this;
  }

  /**
   * Sets the retry reader callable.
   *
   * <p>Used for retry support when eventList appears early in the document.
   *
   * @param retryReader callable that provides a fresh reader
   * @return this builder
   */
  public ObjectNodePublisherBuilder<T> retryReader(Callable<Reader> retryReader) {
    this.retryReader = retryReader;
    return this;
  }

  /**
   * Sets the minimum events per second threshold for spillover.
   *
   * <p><strong>Default: -1 (DISABLED)</strong>
   *
   * <p>When set to -1 (default), spillover is disabled. When set to >= 0, spillover is enabled
   * and will trigger if consumption rate drops below this value.
   *
   * @param minEventsPerSecond minimum rate threshold, or -1 to disable spillover
   * @return this builder
   */
  public ObjectNodePublisherBuilder<T> minEventsPerSecond(double minEventsPerSecond) {
    configBuilder.minEventsPerSecond(minEventsPerSecond);
    return this;
  }

  /**
   * Sets the delay before starting to check consumption rate.
   *
   * <p><strong>Default: 5000ms (5 seconds)</strong>
   *
   * @param delayMs delay in milliseconds
   * @return this builder
   */
  public ObjectNodePublisherBuilder<T> spilloverCheckDelay(long delayMs) {
    configBuilder.spilloverCheckDelayMs(delayMs);
    return this;
  }

  /**
   * Sets whether to spill already-parsed events to disk.
   *
   * <p><strong>Default: false</strong>
   *
   * @param spillParsedEvents true to spill parsed events, false to keep them in memory
   * @return this builder
   */
  public ObjectNodePublisherBuilder<T> spillParsedEvents(boolean spillParsedEvents) {
    configBuilder.spillParsedEvents(spillParsedEvents);
    return this;
  }

  /**
   * Sets the directory for temporary spillover files.
   *
   * <p><strong>Default: null (system temp directory)</strong>
   *
   * @param tempDirectory directory for temp files, or null for system temp
   * @return this builder
   */
  public ObjectNodePublisherBuilder<T> tempDirectory(Path tempDirectory) {
    configBuilder.tempDirectory(tempDirectory);
    return this;
  }

  /**
   * Sets whether to automatically delete temporary files after processing.
   *
   * <p><strong>Default: true</strong>
   *
   * @param autoDelete true to auto-delete, false to preserve temp files
   * @return this builder
   */
  public ObjectNodePublisherBuilder<T> autoDeleteTempFile(boolean autoDelete) {
    configBuilder.autoDeleteTempFile(autoDelete);
    return this;
  }

  /**
   * Builds the ObjectNodePublisher with the configured settings.
   *
   * @return new ObjectNodePublisher instance
   * @throws IOException if publisher initialization fails
   * @throws IllegalStateException if configuration is invalid
   */
  @SuppressWarnings("unchecked")
  public ObjectNodePublisher<T> build() throws IOException {
    validate();

    PublisherConfig config = configBuilder.build();
    config.validate();

    if (inputStream != null) {
      return (ObjectNodePublisher<T>) new ObjectNodePublisher<>(inputStream, retryInputStream, config);
    } else {
      return (ObjectNodePublisher<T>) new ObjectNodePublisher<>(reader, retryReader, config);
    }
  }

  /**
   * Validates the builder configuration.
   *
   * @throws IllegalStateException if configuration is invalid
   */
  private void validate() {
    if (inputStream == null && reader == null) {
      throw new IllegalStateException("Either inputStream or reader must be set");
    }
    if (inputStream != null && reader != null) {
      throw new IllegalStateException("Cannot set both inputStream and reader");
    }
    if (retryInputStream != null && retryReader != null) {
      throw new IllegalStateException("Cannot set both retryInputStream and retryReader");
    }
    if (inputStream != null && retryReader != null) {
      throw new IllegalStateException("retryReader can only be used with reader, not inputStream");
    }
    if (reader != null && retryInputStream != null) {
      throw new IllegalStateException("retryInputStream can only be used with inputStream, not reader");
    }
  }
}
