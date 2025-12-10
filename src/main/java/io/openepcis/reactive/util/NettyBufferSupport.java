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
import java.util.concurrent.Flow;

/**
 * Optional Netty ByteBuf support for zero-copy buffer operations.
 *
 * <p>This class provides more efficient buffer management for high-throughput scenarios
 * by leveraging Netty's pooled buffer allocator and zero-copy operations.
 *
 * <p><strong>Availability:</strong> This class only works if Netty is on the classpath.
 * Check availability via {@link #isAvailable()} before using Netty-specific methods.
 *
 * <p><strong>To enable, add netty-buffer to your dependencies:</strong>
 * <pre>{@code
 * <dependency>
 *     <groupId>io.netty</groupId>
 *     <artifactId>netty-buffer</artifactId>
 * </dependency>
 * }</pre>
 *
 * <p><strong>Usage:</strong>
 * <pre>{@code
 * if (NettyBufferSupport.isAvailable()) {
 *     Multi<io.netty.buffer.ByteBuf> nettyStream =
 *         NettyBufferSupport.toNettyBuffers(byteArrayMulti);
 *
 *     nettyStream.subscribe().with(
 *         buf -> {
 *             try {
 *                 // Process ByteBuf directly without copying
 *                 channel.write(buf);
 *             } finally {
 *                 buf.release();  // IMPORTANT: Release to prevent leaks
 *             }
 *         });
 * }
 * }</pre>
 *
 * <p><strong>Important:</strong> When using Netty ByteBufs, consumers MUST call
 * {@code ByteBuf.release()} on each buffer to prevent memory leaks.
 *
 * @see ReactiveSource
 * @see ByteBufferChunker
 */
public final class NettyBufferSupport {

  private static final boolean NETTY_AVAILABLE = isNettyOnClasspath();

  private NettyBufferSupport() {
    // Utility class
  }

  private static boolean isNettyOnClasspath() {
    try {
      Class.forName("io.netty.buffer.ByteBuf");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * Checks if Netty buffer support is available.
   *
   * <p>Call this method before using any Netty-specific methods to ensure
   * graceful degradation when Netty is not on the classpath.
   *
   * @return true if Netty is available on the classpath
   */
  public static boolean isAvailable() {
    return NETTY_AVAILABLE;
  }

  // ==================== Multi Conversion: byte[] ↔ ByteBuf ====================

  /**
   * Creates a Multi that emits Netty ByteBuf chunks instead of byte arrays.
   *
   * <p><strong>Important:</strong> ByteBufs MUST be released by the consumer
   * by calling {@code ByteBuf.release()} after processing each buffer.
   *
   * @param source the source Multi emitting byte arrays
   * @return Multi emitting ByteBuf instances wrapping the byte arrays
   * @throws IllegalStateException if Netty is not available
   */
  public static Multi<io.netty.buffer.ByteBuf> toNettyBuffers(Multi<byte[]> source) {
    checkAvailable();
    return source.onItem().transform(bytes ->
        io.netty.buffer.Unpooled.wrappedBuffer(bytes));
  }

  /**
   * Creates a Multi that emits Netty ByteBuf chunks using pooled allocation.
   *
   * <p>Pooled buffers provide better performance for high-throughput scenarios
   * by reusing buffer memory from a pool instead of allocating new arrays.
   *
   * <p><strong>Important:</strong> ByteBufs MUST be released by the consumer
   * by calling {@code ByteBuf.release()} after processing each buffer.
   *
   * @param source the source Multi emitting byte arrays
   * @return Multi emitting ByteBuf instances using pooled allocation
   * @throws IllegalStateException if Netty is not available
   */
  public static Multi<io.netty.buffer.ByteBuf> toPooledNettyBuffers(Multi<byte[]> source) {
    checkAvailable();
    return source.onItem().transform(bytes -> {
      io.netty.buffer.ByteBuf buf = io.netty.buffer.PooledByteBufAllocator.DEFAULT.buffer(bytes.length);
      buf.writeBytes(bytes);
      return buf;
    });
  }

  /**
   * Converts a ByteBuf Multi back to byte array Multi with proper release.
   *
   * <p>This method handles the release of ByteBufs automatically, so consumers
   * don't need to manage buffer lifecycle.
   *
   * @param source the source Multi emitting ByteBufs
   * @return Multi emitting byte arrays
   * @throws IllegalStateException if Netty is not available
   */
  public static Multi<byte[]> fromNettyBuffers(Multi<io.netty.buffer.ByteBuf> source) {
    checkAvailable();
    return source.onItem().transform(buf -> {
      try {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return bytes;
      } finally {
        buf.release();
      }
    });
  }

  // ==================== Multi Conversion: ByteBuffer ↔ ByteBuf ====================

  /**
   * Converts a ByteBuf Multi to a ByteBuffer Multi with proper release.
   *
   * <p>This method copies data to ensure the ByteBuffer remains valid after
   * the ByteBuf is released.
   *
   * @param source the source Multi emitting ByteBufs
   * @return Multi emitting ByteBuffers
   * @throws IllegalStateException if Netty is not available
   */
  public static Multi<ByteBuffer> toByteBuffers(Multi<io.netty.buffer.ByteBuf> source) {
    checkAvailable();
    return source.onItem().transform(buf -> {
      try {
        ByteBuffer nioBuffer = buf.nioBuffer();
        // Copy to ensure memory safety after release
        ByteBuffer copy = ByteBuffer.allocate(nioBuffer.remaining());
        copy.put(nioBuffer);
        copy.flip();
        return copy;
      } finally {
        buf.release();
      }
    });
  }

  /**
   * Converts a ByteBuffer Multi to a ByteBuf Multi.
   *
   * <p><strong>Important:</strong> ByteBufs MUST be released by the consumer.
   *
   * @param source the source Multi emitting ByteBuffers
   * @return Multi emitting ByteBufs wrapping the ByteBuffer data
   * @throws IllegalStateException if Netty is not available
   */
  public static Multi<io.netty.buffer.ByteBuf> fromByteBuffers(Multi<ByteBuffer> source) {
    checkAvailable();
    return source.onItem().transform(buffer -> {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return io.netty.buffer.Unpooled.wrappedBuffer(bytes);
    });
  }

  /**
   * Converts a ByteBuffer Multi to a pooled ByteBuf Multi.
   *
   * <p><strong>Important:</strong> ByteBufs MUST be released by the consumer.
   *
   * @param source the source Multi emitting ByteBuffers
   * @return Multi emitting pooled ByteBufs
   * @throws IllegalStateException if Netty is not available
   */
  public static Multi<io.netty.buffer.ByteBuf> fromByteBuffersPooled(Multi<ByteBuffer> source) {
    checkAvailable();
    return source.onItem().transform(buffer -> {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      io.netty.buffer.ByteBuf buf =
          io.netty.buffer.PooledByteBufAllocator.DEFAULT.buffer(bytes.length);
      buf.writeBytes(bytes);
      return buf;
    });
  }

  // ==================== Flow.Publisher Adaptation ====================

  /**
   * Adapts a Flow.Publisher of ByteBuf to a Flow.Publisher of ByteBuffer.
   *
   * <p>The adapter handles ByteBuf reference counting - each ByteBuf is released after
   * its content is copied to a ByteBuffer and passed downstream.
   *
   * <p><strong>Memory safety:</strong> All data is copied to ensure the ByteBuffer remains
   * valid after the ByteBuf is released. This prevents use-after-free issues when downstream
   * processing is slow.
   *
   * @param source the ByteBuf publisher
   * @return adapted ByteBuffer publisher
   * @throws IllegalArgumentException if source is null
   * @throws IllegalStateException if Netty is not available
   */
  public static Flow.Publisher<ByteBuffer> adaptToByteBuffer(
      Flow.Publisher<io.netty.buffer.ByteBuf> source) {
    if (source == null) {
      throw new IllegalArgumentException("Source publisher cannot be null");
    }
    checkAvailable();

    return subscriber -> source.subscribe(new Flow.Subscriber<io.netty.buffer.ByteBuf>() {
      private Flow.Subscription upstream;

      @Override
      public void onSubscribe(Flow.Subscription subscription) {
        this.upstream = subscription;
        subscriber.onSubscribe(new Flow.Subscription() {
          @Override
          public void request(long n) {
            upstream.request(n);
          }

          @Override
          public void cancel() {
            upstream.cancel();
          }
        });
      }

      @Override
      public void onNext(io.netty.buffer.ByteBuf buf) {
        try {
          int readableBytes = buf.readableBytes();
          if (readableBytes == 0) {
            subscriber.onNext(ByteBuffer.allocate(0));
          } else {
            // Copy to ensure memory safety after release
            byte[] bytes = new byte[readableBytes];
            buf.getBytes(buf.readerIndex(), bytes);
            subscriber.onNext(ByteBuffer.wrap(bytes));
          }
        } finally {
          buf.release();
        }
      }

      @Override
      public void onError(Throwable throwable) {
        subscriber.onError(throwable);
      }

      @Override
      public void onComplete() {
        subscriber.onComplete();
      }
    });
  }

  // ==================== Direct Buffer Operations ====================

  /**
   * Wraps a byte array as a Netty ByteBuf without copying.
   *
   * <p><strong>Important:</strong> The returned ByteBuf shares the byte array's memory.
   * Modifying the byte array will affect the ByteBuf and vice versa.
   *
   * @param bytes the byte array to wrap
   * @return a ByteBuf wrapping the byte array
   * @throws IllegalStateException if Netty is not available
   */
  public static io.netty.buffer.ByteBuf wrap(byte[] bytes) {
    checkAvailable();
    return io.netty.buffer.Unpooled.wrappedBuffer(bytes);
  }

  /**
   * Wraps a ByteBuffer as a Netty ByteBuf without copying.
   *
   * <p><strong>Important:</strong> The returned ByteBuf shares the ByteBuffer's memory.
   *
   * @param buffer the ByteBuffer to wrap
   * @return a ByteBuf wrapping the ByteBuffer
   * @throws IllegalStateException if Netty is not available
   */
  public static io.netty.buffer.ByteBuf wrap(ByteBuffer buffer) {
    checkAvailable();
    return io.netty.buffer.Unpooled.wrappedBuffer(buffer);
  }

  /**
   * Allocates a pooled ByteBuf with the specified capacity.
   *
   * @param capacity the initial capacity
   * @return a pooled ByteBuf
   * @throws IllegalStateException if Netty is not available
   */
  public static io.netty.buffer.ByteBuf allocate(int capacity) {
    checkAvailable();
    return io.netty.buffer.PooledByteBufAllocator.DEFAULT.buffer(capacity);
  }

  /**
   * Allocates an unpooled ByteBuf with the specified capacity.
   *
   * @param capacity the initial capacity
   * @return an unpooled ByteBuf
   * @throws IllegalStateException if Netty is not available
   */
  public static io.netty.buffer.ByteBuf allocateUnpooled(int capacity) {
    checkAvailable();
    return io.netty.buffer.Unpooled.buffer(capacity);
  }

  /**
   * Allocates a direct (off-heap) pooled ByteBuf with the specified capacity.
   *
   * @param capacity the initial capacity
   * @return a direct pooled ByteBuf
   * @throws IllegalStateException if Netty is not available
   */
  public static io.netty.buffer.ByteBuf allocateDirect(int capacity) {
    checkAvailable();
    return io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer(capacity);
  }

  // ==================== Availability Check ====================

  /**
   * Checks if Netty is available and throws if not.
   *
   * <p>This method can be called by other modules that need to verify Netty availability
   * before using Netty-specific features.
   *
   * @throws IllegalStateException if Netty is not available
   */
  public static void checkAvailable() {
    if (!NETTY_AVAILABLE) {
      throw new IllegalStateException(
          "Netty ByteBuf support requires netty-buffer dependency. " +
          "Add io.netty:netty-buffer to your project.");
    }
  }
}
