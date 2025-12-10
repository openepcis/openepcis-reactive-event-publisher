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
import io.smallrye.mutiny.subscription.MultiEmitter;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility for chunking byte streams at fixed boundaries for efficient network transmission.
 *
 * <p>This class provides methods to split byte arrays, ByteBuffers, and reactive
 * streams into consistent chunk sizes (default 8KB) for optimal network transmission.
 *
 * <p><strong>Usage with Multi:</strong>
 * <pre>{@code
 * // Chunk a Multi stream
 * Multi<byte[]> chunkedOutput = sourceMulti
 *     .plug(ByteBufferChunker::chunkBytes);
 *
 * // Chunk a byte array synchronously
 * List<byte[]> chunks = ByteBufferChunker.chunk(largeByteArray, 8192);
 *
 * // Convert to ByteBuffer Multi
 * Multi<ByteBuffer> buffers = ByteBufferChunker.toByteBuffers(chunkedOutput);
 * }</pre>
 *
 * <p><strong>Thread safety:</strong> The static {@code chunkBytes()} methods are thread-safe
 * and create isolated state per subscription. Instance methods are NOT thread-safe and
 * should be used from a single thread.
 */
public final class ByteBufferChunker {

  /** Default chunk size of 8KB */
  public static final int DEFAULT_CHUNK_SIZE = 8192;

  private final int chunkSize;
  private final ByteArrayOutputStream buffer;

  /**
   * Creates a new chunker with default 8KB chunk size.
   */
  public ByteBufferChunker() {
    this(DEFAULT_CHUNK_SIZE);
  }

  /**
   * Creates a new chunker with specified chunk size.
   *
   * @param chunkSize the chunk size in bytes
   * @throws IllegalArgumentException if chunkSize is not positive
   */
  public ByteBufferChunker(int chunkSize) {
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("Chunk size must be positive: " + chunkSize);
    }
    this.chunkSize = chunkSize;
    this.buffer = new ByteArrayOutputStream(chunkSize);
  }

  // ==================== Synchronous Chunking ====================

  /**
   * Chunks a byte array into fixed-size chunks.
   *
   * @param data the byte array to chunk
   * @param chunkSize the maximum size of each chunk
   * @return list of byte array chunks
   * @throws IllegalArgumentException if chunkSize is not positive
   */
  public static List<byte[]> chunk(byte[] data, int chunkSize) {
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("Chunk size must be positive: " + chunkSize);
    }
    if (data == null || data.length == 0) {
      return List.of();
    }

    List<byte[]> chunks = new ArrayList<>();
    int offset = 0;

    while (offset < data.length) {
      int length = Math.min(chunkSize, data.length - offset);
      byte[] chunk = new byte[length];
      System.arraycopy(data, offset, chunk, 0, length);
      chunks.add(chunk);
      offset += length;
    }

    return chunks;
  }

  /**
   * Chunks a byte array into default 8KB chunks.
   *
   * @param data the byte array to chunk
   * @return list of byte array chunks
   */
  public static List<byte[]> chunk(byte[] data) {
    return chunk(data, DEFAULT_CHUNK_SIZE);
  }

  // ==================== Reactive Chunking: Multi<byte[]> ====================

  /**
   * Transforms a Multi of byte arrays into chunked output at 8KB boundaries.
   *
   * <p>This is the primary entry point for use with Mutiny's plug operator.
   *
   * @param source the source Multi of byte arrays
   * @return Multi emitting byte arrays in approximately 8KB chunks
   */
  public static Multi<byte[]> chunkBytes(Multi<byte[]> source) {
    return chunkBytes(source, DEFAULT_CHUNK_SIZE);
  }

  /**
   * Transforms a Multi of byte arrays into chunked output at specified boundaries.
   *
   * <p>Each subscription gets its own buffer, making this method safe for concurrent use.
   *
   * @param source the source Multi of byte arrays
   * @param chunkSize the target chunk size in bytes
   * @return Multi emitting byte arrays in approximately chunkSize chunks
   */
  public static Multi<byte[]> chunkBytes(Multi<byte[]> source, int chunkSize) {
    // Use emitter pattern - creates isolated buffer per subscription
    return Multi.createFrom().emitter(emitter -> {
      // Buffer is created fresh for each subscription - no shared state
      ByteArrayOutputStream buffer = new ByteArrayOutputStream(chunkSize);

      source.subscribe().with(
          bytes -> emitChunksToEmitter(bytes, buffer, chunkSize, emitter),
          emitter::fail,
          () -> {
            // Emit any remaining buffered data on completion
            if (buffer.size() > 0) {
              emitter.emit(buffer.toByteArray());
            }
            emitter.complete();
          }
      );
    });
  }

  /**
   * Processes input bytes, emitting complete chunks and buffering remainder.
   */
  private static void emitChunksToEmitter(
      byte[] input,
      ByteArrayOutputStream buffer,
      int chunkSize,
      MultiEmitter<? super byte[]> emitter) {

    int inputOffset = 0;
    int remaining = input.length;

    while (remaining > 0) {
      int bufferSpace = chunkSize - buffer.size();
      int toWrite = Math.min(remaining, bufferSpace);

      buffer.write(input, inputOffset, toWrite);
      inputOffset += toWrite;
      remaining -= toWrite;

      if (buffer.size() >= chunkSize) {
        // Buffer is full, emit chunk
        emitter.emit(buffer.toByteArray());
        buffer.reset();
      }
    }
  }

  // ==================== Reactive Conversion: Multi<ByteBuffer> ====================

  /**
   * Converts a Multi of byte arrays to a Multi of ByteBuffers.
   *
   * @param source the source Multi emitting byte arrays
   * @return Multi emitting ByteBuffers
   */
  public static Multi<ByteBuffer> toByteBuffers(Multi<byte[]> source) {
    return source.onItem().transform(ByteBuffer::wrap);
  }

  /**
   * Converts a Multi of ByteBuffers to a Multi of byte arrays.
   *
   * @param source the source Multi emitting ByteBuffers
   * @return Multi emitting byte arrays
   */
  public static Multi<byte[]> fromByteBuffers(Multi<ByteBuffer> source) {
    return source.onItem().transform(buffer -> {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return bytes;
    });
  }

  /**
   * Chunks a Multi of ByteBuffers into fixed-size byte array chunks.
   *
   * @param source the source Multi emitting ByteBuffers
   * @param chunkSize the target chunk size
   * @return Multi emitting fixed-size byte array chunks
   */
  public static Multi<byte[]> chunkByteBuffers(Multi<ByteBuffer> source, int chunkSize) {
    return chunkBytes(fromByteBuffers(source), chunkSize);
  }

  /**
   * Chunks a Multi of ByteBuffers into default 8KB byte array chunks.
   *
   * @param source the source Multi emitting ByteBuffers
   * @return Multi emitting 8KB byte array chunks
   */
  public static Multi<byte[]> chunkByteBuffers(Multi<ByteBuffer> source) {
    return chunkByteBuffers(source, DEFAULT_CHUNK_SIZE);
  }

  /**
   * Chunks and converts a Multi of ByteBuffers to chunked ByteBuffers.
   *
   * @param source the source Multi emitting ByteBuffers
   * @param chunkSize the target chunk size
   * @return Multi emitting fixed-size ByteBuffers
   */
  public static Multi<ByteBuffer> chunkToByteBuffers(Multi<ByteBuffer> source, int chunkSize) {
    return toByteBuffers(chunkByteBuffers(source, chunkSize));
  }

  /**
   * Chunks and converts a Multi of ByteBuffers to default 8KB chunked ByteBuffers.
   *
   * @param source the source Multi emitting ByteBuffers
   * @return Multi emitting 8KB ByteBuffers
   */
  public static Multi<ByteBuffer> chunkToByteBuffers(Multi<ByteBuffer> source) {
    return chunkToByteBuffers(source, DEFAULT_CHUNK_SIZE);
  }

  // ==================== Instance Methods ====================

  /**
   * Adds bytes to the internal buffer and returns any complete chunks.
   *
   * <p>This method is for manual chunking outside of the reactive stream context.
   *
   * @param bytes the bytes to add
   * @return Multi of complete chunks (may be empty if buffer not yet full)
   */
  public Multi<byte[]> add(byte[] bytes) {
    return Multi.createFrom().emitter(emitter -> {
      int inputOffset = 0;
      int remaining = bytes.length;

      while (remaining > 0) {
        int bufferSpace = chunkSize - buffer.size();
        int toWrite = Math.min(remaining, bufferSpace);

        buffer.write(bytes, inputOffset, toWrite);
        inputOffset += toWrite;
        remaining -= toWrite;

        if (buffer.size() >= chunkSize) {
          emitter.emit(buffer.toByteArray());
          buffer.reset();
        }
      }

      emitter.complete();
    });
  }

  /**
   * Flushes any remaining bytes in the buffer.
   *
   * @return the remaining bytes, or empty array if buffer is empty
   */
  public byte[] flush() {
    if (buffer.size() == 0) {
      return new byte[0];
    }
    byte[] result = buffer.toByteArray();
    buffer.reset();
    return result;
  }

  /**
   * Returns the current number of bytes buffered.
   *
   * @return buffered byte count
   */
  public int bufferedSize() {
    return buffer.size();
  }

  /**
   * Resets the internal buffer.
   */
  public void reset() {
    buffer.reset();
  }

  /**
   * Returns the configured chunk size.
   *
   * @return chunk size in bytes
   */
  public int chunkSize() {
    return chunkSize;
  }

  // ==================== Netty ByteBuf Support ====================

  /**
   * Chunks and converts a Multi of byte arrays to Netty ByteBufs.
   *
   * <p><strong>Important:</strong> Consumers MUST call {@code ByteBuf.release()}
   * on each emitted buffer to prevent memory leaks.
   *
   * @param source the source Multi emitting byte arrays
   * @param chunkSize the target chunk size
   * @return Multi emitting Netty ByteBufs
   * @throws IllegalStateException if Netty is not available
   */
  public static Multi<io.netty.buffer.ByteBuf> chunkToNettyBuffers(
      Multi<byte[]> source, int chunkSize) {
    NettyBufferSupport.checkAvailable();
    return chunkBytes(source, chunkSize)
        .onItem().transform(io.netty.buffer.Unpooled::wrappedBuffer);
  }

  /**
   * Chunks and converts a Multi of byte arrays to default 8KB Netty ByteBufs.
   *
   * @param source the source Multi emitting byte arrays
   * @return Multi emitting 8KB Netty ByteBufs
   * @throws IllegalStateException if Netty is not available
   */
  public static Multi<io.netty.buffer.ByteBuf> chunkToNettyBuffers(Multi<byte[]> source) {
    return chunkToNettyBuffers(source, DEFAULT_CHUNK_SIZE);
  }

  /**
   * Chunks and converts a Multi of byte arrays to pooled Netty ByteBufs.
   *
   * <p>Pooled buffers provide better performance for high-throughput scenarios.
   *
   * <p><strong>Important:</strong> Consumers MUST call {@code ByteBuf.release()}
   * on each emitted buffer to prevent memory leaks.
   *
   * @param source the source Multi emitting byte arrays
   * @param chunkSize the target chunk size
   * @return Multi emitting pooled Netty ByteBufs
   * @throws IllegalStateException if Netty is not available
   */
  public static Multi<io.netty.buffer.ByteBuf> chunkToPooledNettyBuffers(
      Multi<byte[]> source, int chunkSize) {
    NettyBufferSupport.checkAvailable();
    return chunkBytes(source, chunkSize)
        .onItem().transform(bytes -> {
          io.netty.buffer.ByteBuf buf =
              io.netty.buffer.PooledByteBufAllocator.DEFAULT.buffer(bytes.length);
          buf.writeBytes(bytes);
          return buf;
        });
  }

  /**
   * Chunks and converts a Multi of byte arrays to default 8KB pooled Netty ByteBufs.
   *
   * @param source the source Multi emitting byte arrays
   * @return Multi emitting 8KB pooled Netty ByteBufs
   * @throws IllegalStateException if Netty is not available
   */
  public static Multi<io.netty.buffer.ByteBuf> chunkToPooledNettyBuffers(Multi<byte[]> source) {
    return chunkToPooledNettyBuffers(source, DEFAULT_CHUNK_SIZE);
  }
}
