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
package io.openepcis.reactive.util;

import io.smallrye.mutiny.Multi;
import java.nio.ByteBuffer;

/**
 * Optional Vert.x Buffer support for Quarkus REST integration.
 *
 * <p>This class provides conversion utilities between byte arrays/ByteBuffers and
 * Vert.x Buffers, which are the preferred type for streaming in Quarkus REST.
 *
 * <p><strong>Availability:</strong> This class only works if Vert.x is on the classpath.
 * Check availability via {@link #isAvailable()} before using Vert.x-specific methods.
 *
 * <p><strong>Usage with Quarkus REST:</strong>
 * <pre>{@code
 * if (VertxBufferSupport.isAvailable()) {
 *     Multi<io.vertx.core.buffer.Buffer> vertxStream =
 *         VertxBufferSupport.toVertxBuffers(byteArrayMulti);
 *
 *     return RestMulti.fromMultiData(vertxStream).build();
 * }
 * }</pre>
 *
 * @see NettyBufferSupport
 */
public final class VertxBufferSupport {

  private static final boolean VERTX_AVAILABLE = isVertxOnClasspath();

  private VertxBufferSupport() {
    // Utility class
  }

  private static boolean isVertxOnClasspath() {
    try {
      Class.forName("io.vertx.core.buffer.Buffer");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * Checks if Vert.x buffer support is available.
   *
   * <p>Call this method before using any Vert.x-specific methods to ensure
   * graceful degradation when Vert.x is not on the classpath.
   *
   * @return true if Vert.x is available on the classpath
   */
  public static boolean isAvailable() {
    return VERTX_AVAILABLE;
  }

  // ==================== Multi Conversion: byte[] ↔ Buffer ====================

  /**
   * Creates a Multi that emits Vert.x Buffer chunks instead of byte arrays.
   *
   * <p>This is the recommended output type for Quarkus REST streaming endpoints.
   *
   * @param source the source Multi emitting byte arrays
   * @return Multi emitting Buffer instances wrapping the byte arrays
   * @throws IllegalStateException if Vert.x is not available
   */
  public static Multi<io.vertx.core.buffer.Buffer> toVertxBuffers(Multi<byte[]> source) {
    checkAvailable();
    return source.onItem().transform(bytes ->
        io.vertx.core.buffer.Buffer.buffer(bytes));
  }

  /**
   * Converts a Buffer Multi back to byte array Multi.
   *
   * @param source the source Multi emitting Buffers
   * @return Multi emitting byte arrays
   * @throws IllegalStateException if Vert.x is not available
   */
  public static Multi<byte[]> fromVertxBuffers(Multi<io.vertx.core.buffer.Buffer> source) {
    checkAvailable();
    return source.onItem().transform(buf -> buf.getBytes());
  }

  // ==================== Multi Conversion: ByteBuffer ↔ Buffer ====================

  /**
   * Creates a Multi that emits Vert.x Buffer chunks from ByteBuffer Multi.
   *
   * @param source the source Multi emitting ByteBuffers
   * @return Multi emitting Buffer instances
   * @throws IllegalStateException if Vert.x is not available
   */
  public static Multi<io.vertx.core.buffer.Buffer> toVertxBuffersFromByteBuffer(Multi<ByteBuffer> source) {
    checkAvailable();
    return source.onItem().transform(buffer -> {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return io.vertx.core.buffer.Buffer.buffer(bytes);
    });
  }

  /**
   * Converts a Buffer Multi to a ByteBuffer Multi.
   *
   * @param source the source Multi emitting Buffers
   * @return Multi emitting ByteBuffers
   * @throws IllegalStateException if Vert.x is not available
   */
  public static Multi<ByteBuffer> toByteBuffers(Multi<io.vertx.core.buffer.Buffer> source) {
    checkAvailable();
    return source.onItem().transform(buf -> ByteBuffer.wrap(buf.getBytes()));
  }

  // ==================== Direct Buffer Operations ====================

  /**
   * Wraps a byte array as a Vert.x Buffer.
   *
   * <p><strong>Note:</strong> Unlike Netty ByteBuf, Vert.x Buffer copies the data
   * by default, so there's no shared memory concern.
   *
   * @param bytes the byte array to wrap
   * @return a Buffer containing the byte array data
   * @throws IllegalStateException if Vert.x is not available
   */
  public static io.vertx.core.buffer.Buffer wrap(byte[] bytes) {
    checkAvailable();
    return io.vertx.core.buffer.Buffer.buffer(bytes);
  }

  /**
   * Wraps a ByteBuffer as a Vert.x Buffer.
   *
   * @param buffer the ByteBuffer to convert
   * @return a Buffer containing the ByteBuffer data
   * @throws IllegalStateException if Vert.x is not available
   */
  public static io.vertx.core.buffer.Buffer wrap(ByteBuffer buffer) {
    checkAvailable();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return io.vertx.core.buffer.Buffer.buffer(bytes);
  }

  /**
   * Creates an empty Vert.x Buffer.
   *
   * @return an empty Buffer
   * @throws IllegalStateException if Vert.x is not available
   */
  public static io.vertx.core.buffer.Buffer buffer() {
    checkAvailable();
    return io.vertx.core.buffer.Buffer.buffer();
  }

  /**
   * Creates a Vert.x Buffer with the specified initial capacity.
   *
   * @param initialCapacity the initial capacity
   * @return a Buffer with the specified capacity
   * @throws IllegalStateException if Vert.x is not available
   */
  public static io.vertx.core.buffer.Buffer buffer(int initialCapacity) {
    checkAvailable();
    return io.vertx.core.buffer.Buffer.buffer(initialCapacity);
  }

  // ==================== Availability Check ====================

  /**
   * Checks if Vert.x is available and throws if not.
   *
   * @throws IllegalStateException if Vert.x is not available
   */
  public static void checkAvailable() {
    if (!VERTX_AVAILABLE) {
      throw new IllegalStateException(
          "Vert.x Buffer support requires vertx-core dependency. " +
          "Add io.vertx:vertx-core to your project.");
    }
  }
}
