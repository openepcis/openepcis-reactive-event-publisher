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

import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;

/**
 * A transparent InputStream wrapper that supports on-demand spillover to temporary files.
 *
 * <p>This class wraps an original InputStream and tracks how many bytes have been read. When
 * spillover is triggered via {@link #spillRemaining(SpilloverManager)}, it:
 * <ol>
 *   <li>Copies all remaining unread bytes from the original stream to a temp file</li>
 *   <li>Closes the original stream gracefully</li>
 *   <li>Switches to reading from the temp file transparently</li>
 * </ol>
 *
 * <p>This allows a JsonParser (or any consumer) to continue reading seamlessly while the
 * underlying stream source changes from network/file to disk-based temp file.
 *
 * <p><strong>Thread-safety:</strong> This class is NOT thread-safe. It should only be accessed
 * from a single thread (the parsing thread).
 *
 * <p>Package-private - internal implementation detail of ObjectNodePublisher.
 */
@Slf4j
class SpillableInputStream extends InputStream {
  private InputStream currentStream;
  private final InputStream originalStream;
  private long bytesRead = 0;
  private boolean spilledOver = false;

  /**
   * Creates a new spillable input stream wrapping the original stream.
   *
   * @param originalStream the stream to wrap (must not be null)
   */
  SpillableInputStream(InputStream originalStream) {
    if (originalStream == null) {
      throw new IllegalArgumentException("Original stream cannot be null");
    }
    this.originalStream = originalStream;
    this.currentStream = originalStream;
  }

  /**
   * Checks if spillover has been triggered.
   *
   * @return true if stream has been spilled to temp file
   */
  boolean isSpilledOver() {
    return spilledOver;
  }

  /**
   * Gets the number of bytes read so far from this stream.
   *
   * @return total bytes read
   */
  long getBytesRead() {
    return bytesRead;
  }

  /**
   * Triggers spillover - copies remaining bytes to temp file and switches stream.
   *
   * <p>This method:
   * <ol>
   *   <li>Calls {@link SpilloverManager#spillRemainingInput(InputStream)} with the original stream</li>
   *   <li>The manager copies remaining bytes and closes the original stream</li>
   *   <li>Gets a new InputStream from the temp file via {@link SpilloverManager#getTempInputStream()}</li>
   *   <li>Switches currentStream to the temp file stream</li>
   * </ol>
   *
   * <p>After this method completes, all subsequent read() calls will transparently read from
   * the temp file. The original stream is closed and can be garbage collected.
   *
   * @param manager the spillover manager to use for creating temp file
   * @throws IOException if spillover fails
   * @throws IllegalStateException if already spilled over
   */
  void spillRemaining(SpilloverManager manager) throws IOException {
    if (spilledOver) {
      throw new IllegalStateException("Stream has already been spilled over");
    }

    log.info("SpillableInputStream: Starting spillover after {} bytes read", bytesRead);

    // Spill remaining bytes and close original stream
    manager.spillRemainingInput(originalStream);

    // Switch to temp file stream
    currentStream = manager.getTempInputStream();

    spilledOver = true;

    log.info("SpillableInputStream: Spillover complete, now reading from temp file");
  }

  @Override
  public int read() throws IOException {
    int b = currentStream.read();
    if (b != -1) {
      bytesRead++;
    }
    return b;
  }

  @Override
  public int read(byte[] b) throws IOException {
    int count = currentStream.read(b);
    if (count > 0) {
      bytesRead += count;
    }
    return count;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int count = currentStream.read(b, off, len);
    if (count > 0) {
      bytesRead += count;
    }
    return count;
  }

  @Override
  public long skip(long n) throws IOException {
    long skipped = currentStream.skip(n);
    if (skipped > 0) {
      bytesRead += skipped;
    }
    return skipped;
  }

  @Override
  public int available() throws IOException {
    return currentStream.available();
  }

  @Override
  public void close() throws IOException {
    // Close whatever stream is currently active
    if (currentStream != null) {
      currentStream.close();
    }
  }

  @Override
  public synchronized void mark(int readlimit) {
    currentStream.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    currentStream.reset();
  }

  @Override
  public boolean markSupported() {
    return currentStream.markSupported();
  }

  /**
   * Gets diagnostic information about this stream.
   *
   * @return diagnostic string
   */
  String getDiagnostics() {
    return String.format(
        "SpillableInputStream[bytesRead=%d, spilledOver=%b, currentStream=%s]",
        bytesRead, spilledOver, currentStream.getClass().getSimpleName());
  }
}
