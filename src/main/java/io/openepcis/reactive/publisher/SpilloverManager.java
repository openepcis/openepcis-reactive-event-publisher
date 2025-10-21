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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Queue;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages spillover of input streams and parsed events to temporary files.
 *
 * <p>When subscriber consumption is slower than input stream reading, this class spills the
 * remaining unparsed bytes (and optionally parsed events) to a temporary file. This prevents
 * blocking slow network/HTTP streams for extended periods.
 *
 * <p><strong>EAGER spillover strategy:</strong> As soon as the consumption rate drops below the
 * threshold, ALL remaining input is immediately copied to disk.
 *
 * <p><strong>Lifecycle:</strong>
 * <ol>
 *   <li>Create SpilloverManager with configuration</li>
 *   <li>Call {@link #spillRemainingInput(InputStream)} to copy unparsed bytes to temp file</li>
 *   <li>Optionally call {@link #spillParsedEvents(Queue, ObjectMapper)} to serialize parsed events</li>
 *   <li>Call {@link #getTempInputStream()} to get stream from temp file</li>
 *   <li>Call {@link #cleanup()} to delete temp file when done</li>
 * </ol>
 *
 * <p>Package-private - internal implementation detail of ObjectNodePublisher.
 */
@Slf4j
class SpilloverManager {
  private static final int BUFFER_SIZE = 65536;  // 64KB buffer for spillover I/O

  private final PublisherConfig config;
  private Path tempFilePath;
  private boolean spilloverTriggered = false;
  private long bytesSpilled = 0;

  /**
   * Creates a new spillover manager.
   *
   * @param config publisher configuration containing spillover settings
   */
  SpilloverManager(PublisherConfig config) {
    this.config = config;
  }

  /**
   * Checks if spillover has been triggered.
   *
   * @return true if spillover has been activated, false otherwise
   */
  boolean isSpilloverTriggered() {
    return spilloverTriggered;
  }

  /**
   * Gets the path to the temporary spillover file.
   *
   * @return temp file path, or null if spillover not triggered
   */
  Path getTempFilePath() {
    return tempFilePath;
  }

  /**
   * Gets the number of bytes spilled to the temporary file.
   *
   * @return bytes spilled
   */
  long getBytesSpilled() {
    return bytesSpilled;
  }

  /**
   * Spills all remaining bytes from the input stream to a temporary file.
   *
   * <p><strong>EAGER spillover:</strong> This method reads ALL remaining bytes from the input
   * stream immediately and writes them to a temp file. The original input stream is then closed.
   *
   * <p>The temporary file is created with a unique name in the configured temp directory:
   * {@code epcis-spillover-{uuid}.json}
   *
   * @param inputStream the input stream to spill (will be closed after spillover)
   * @return path to the temporary file containing the spilled data
   * @throws IOException if spillover fails
   */
  Path spillRemainingInput(InputStream inputStream) throws IOException {
    if (spilloverTriggered) {
      throw new IllegalStateException("Spillover already triggered");
    }

    // Create temp file
    String fileName = "epcis-spillover-" + UUID.randomUUID() + ".json";
    Path tempDir = config.getTempDirectory() != null
        ? config.getTempDirectory()
        : Files.createTempDirectory("openepcis");

    tempFilePath = tempDir.resolve(fileName);

    log.info("Triggering EAGER spillover - copying remaining input to temp file: {}", tempFilePath);

    // Copy all remaining bytes to temp file
    long startTime = System.currentTimeMillis();
    try (BufferedOutputStream out = new BufferedOutputStream(
             new FileOutputStream(tempFilePath.toFile()), BUFFER_SIZE)) {

      byte[] buffer = new byte[BUFFER_SIZE];
      int bytesRead;

      while ((bytesRead = inputStream.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
        bytesSpilled += bytesRead;
      }

      out.flush();
    } finally {
      // Close original input stream
      try {
        inputStream.close();
      } catch (IOException e) {
        log.warn("Error closing original input stream after spillover", e);
      }
    }

    long durationMs = System.currentTimeMillis() - startTime;
    long spilledKB = bytesSpilled / 1024;
    long throughputKBps = durationMs > 0 ? (spilledKB * 1000 / durationMs) : 0;

    log.info("Spillover complete - {} KB spilled in {} ms ({} KB/s)",
        spilledKB, durationMs, throughputKBps);

    spilloverTriggered = true;
    return tempFilePath;
  }

  /**
   * Optionally spills already-parsed ObjectNode events to the temporary file.
   *
   * <p>This method re-serializes parsed events as JSON and appends them to the temp file. This
   * allows the publisher to free memory by removing events from the queue.
   *
   * <p><strong>Note:</strong> This is only called if {@code config.spillParsedEvents = true}.
   *
   * @param parsedEvents queue of parsed events to spill
   * @param mapper JSON object mapper for serialization
   * @throws IOException if serialization fails
   */
  void spillParsedEvents(Queue<ObjectNode> parsedEvents, ObjectMapper mapper) throws IOException {
    if (!spilloverTriggered) {
      throw new IllegalStateException("Must call spillRemainingInput() before spillParsedEvents()");
    }

    if (parsedEvents.isEmpty()) {
      log.debug("No parsed events to spill");
      return;
    }

    log.info("Spilling {} parsed events to temp file", parsedEvents.size());

    // Re-open temp file in append mode
    try (BufferedOutputStream out = new BufferedOutputStream(
             new FileOutputStream(tempFilePath.toFile(), true), BUFFER_SIZE)) {

      for (ObjectNode event : parsedEvents) {
        byte[] jsonBytes = mapper.writeValueAsBytes(event);
        out.write(jsonBytes);
        out.write('\n');  // Newline separator
        bytesSpilled += jsonBytes.length + 1;
      }

      out.flush();
    }

    // Clear parsed events from memory
    parsedEvents.clear();

    log.info("Parsed events spilled - {} total bytes in temp file", bytesSpilled);
  }

  /**
   * Gets a new InputStream reading from the temporary spillover file.
   *
   * <p>This stream is automatically buffered for optimal performance.
   *
   * @return buffered input stream from temp file
   * @throws IOException if temp file cannot be opened
   * @throws IllegalStateException if spillover not triggered yet
   */
  InputStream getTempInputStream() throws IOException {
    if (!spilloverTriggered || tempFilePath == null) {
      throw new IllegalStateException("Spillover not triggered - no temp file available");
    }

    return new BufferedInputStream(new FileInputStream(tempFilePath.toFile()), BUFFER_SIZE);
  }

  /**
   * Cleans up the temporary spillover file.
   *
   * <p>If {@code config.autoDeleteTempFile = true} (default), deletes the temp file.
   * Otherwise, logs the file location for manual cleanup.
   *
   * <p>This method is safe to call multiple times - it's a no-op after first call.
   */
  void cleanup() {
    if (tempFilePath == null) {
      return;  // Nothing to clean up
    }

    if (config.isAutoDeleteTempFile()) {
      try {
        if (Files.exists(tempFilePath)) {
          Files.delete(tempFilePath);
          log.debug("Deleted temp spillover file: {}", tempFilePath);
        }
      } catch (IOException e) {
        log.warn("Failed to delete temp spillover file: {}", tempFilePath, e);
      }
    } else {
      log.info("Temp spillover file preserved (autoDeleteTempFile=false): {}", tempFilePath);
    }

    tempFilePath = null;
  }

  /**
   * Gets diagnostic information about the spillover state.
   *
   * @return diagnostic string
   */
  String getDiagnostics() {
    return String.format(
        "SpilloverManager[triggered=%b, tempFile=%s, bytesSpilled=%d KB]",
        spilloverTriggered,
        tempFilePath != null ? tempFilePath : "none",
        bytesSpilled / 1024);
  }
}
