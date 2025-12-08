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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.Flow;

/**
 * Support for Netty ByteBuf integration with AsyncObjectNodeParser.
 *
 * <p>This class provides utilities for feeding Netty ByteBuf instances to the parser
 * with zero-copy optimization when possible, and proper reference counting management.
 *
 * <p><strong>Usage:</strong>
 * <pre>{@code
 * // Direct feeding to parser
 * ByteBuf buf = ...;
 * try {
 *     AsyncObjectNodeParser.FeedResult result = ByteBufSupport.feedByteBuf(parser, buf);
 * } finally {
 *     buf.release();
 * }
 *
 * // Create publisher from ByteBuf source
 * Flow.Publisher<ByteBuf> nettySource = ...;
 * ObjectNodePublisher<ObjectNode> publisher = ByteBufSupport.createPublisher(nettySource);
 * }</pre>
 *
 * <p><strong>Reference counting:</strong> This class does NOT automatically release ByteBuf
 * instances. The caller is responsible for proper reference counting. When using
 * {@link #adaptToByteBuffer(Flow.Publisher)}, the adapter handles release after consumption.
 *
 * <p><strong>Note:</strong> This class requires Netty to be on the classpath. If Netty is not
 * available, methods will throw {@link NoClassDefFoundError}.
 */
public final class ByteBufSupport {

  /** Cached result of Netty availability check for performance. */
  private static final boolean NETTY_AVAILABLE;

  static {
    boolean available;
    try {
      Class.forName("io.netty.buffer.ByteBuf");
      available = true;
    } catch (ClassNotFoundException e) {
      available = false;
    }
    NETTY_AVAILABLE = available;
  }

  private ByteBufSupport() {
    // Utility class
  }

  /**
   * Checks if Netty ByteBuf support is available.
   *
   * @return true if Netty classes are on the classpath
   */
  public static boolean isAvailable() {
    return NETTY_AVAILABLE;
  }

  /**
   * Feeds a ByteBuf to the parser with zero-copy optimization when possible.
   *
   * <p>Zero-copy is used when the ByteBuf has a backing array. For direct buffers or
   * composite buffers without backing array, data is copied.
   *
   * <p><strong>Important:</strong> This method does NOT release the ByteBuf. The caller
   * must manage reference counting.
   *
   * @param parser the async parser to feed
   * @param buf the ByteBuf containing JSON data (will be fully consumed)
   * @return result indicating parser state after feeding
   * @throws IOException if parsing error occurs
   * @throws IllegalArgumentException if parser or buf is null
   */
  public static AsyncObjectNodeParser.FeedResult feedByteBuf(
      AsyncObjectNodeParser parser, ByteBuf buf) throws IOException {
    if (parser == null) {
      throw new IllegalArgumentException("Parser cannot be null");
    }
    if (buf == null) {
      throw new IllegalArgumentException("ByteBuf cannot be null");
    }

    int readableBytes = buf.readableBytes();
    if (readableBytes == 0) {
      return AsyncObjectNodeParser.FeedResult.NEED_MORE_INPUT;
    }

    if (buf.hasArray()) {
      // Zero-copy: use backing array directly
      byte[] array = buf.array();
      int offset = buf.arrayOffset() + buf.readerIndex();
      AsyncObjectNodeParser.FeedResult result = parser.feedInput(array, offset, readableBytes);
      buf.readerIndex(buf.writerIndex()); // Mark as consumed
      return result;
    } else {
      // Copy required: direct buffer or composite without backing array
      byte[] bytes = new byte[readableBytes];
      buf.readBytes(bytes);
      return parser.feedInput(bytes, 0, bytes.length);
    }
  }

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
   */
  public static Flow.Publisher<ByteBuffer> adaptToByteBuffer(Flow.Publisher<ByteBuf> source) {
    if (source == null) {
      throw new IllegalArgumentException("Source publisher cannot be null");
    }

    return subscriber -> source.subscribe(new ByteBufToByteBufferSubscriber(subscriber));
  }

  /**
   * Creates a ObjectNodePublisher from a ByteBuf source.
   *
   * <p>This is a convenience method combining {@link #adaptToByteBuffer} with
   * {@link ObjectNodePublisher}.
   *
   * @param source the ByteBuf publisher
   * @param <T> type of ObjectNode emitted
   * @return new ObjectNodePublisher
   * @throws IOException if publisher initialization fails
   * @throws IllegalArgumentException if source is null
   */
  public static <T extends com.fasterxml.jackson.databind.node.ObjectNode>
      ObjectNodePublisher<T> createPublisher(Flow.Publisher<ByteBuf> source)
          throws IOException {
    return new ObjectNodePublisher<>(adaptToByteBuffer(source));
  }

  /**
   * Creates a ObjectNodePublisher from a ByteBuf source with retry support.
   *
   * @param source the ByteBuf publisher
   * @param retrySource callable for retry (early-eventList handling)
   * @param <T> type of ObjectNode emitted
   * @return new ObjectNodePublisher
   * @throws IOException if publisher initialization fails
   */
  public static <T extends com.fasterxml.jackson.databind.node.ObjectNode>
      ObjectNodePublisher<T> createPublisher(
          Flow.Publisher<ByteBuf> source, Callable<Flow.Publisher<ByteBuf>> retrySource)
              throws IOException {
    Callable<Flow.Publisher<ByteBuffer>> adaptedRetry = retrySource == null
        ? null
        : () -> adaptToByteBuffer(retrySource.call());

    return new ObjectNodePublisher<>(adaptToByteBuffer(source), adaptedRetry);
  }

  /**
   * Creates a ObjectNodePublisher from a Mutiny Multi of ByteBuf.
   *
   * <p>Convenience method for Quarkus/Vert.x applications using Mutiny.
   *
   * <p>Example:
   * <pre>{@code
   * Multi<ByteBuf> nettyBody = ...;
   * ByteBufSupport.fromMulti(nettyBody)
   *     .toMulti()
   *     .subscribe().with(node -> process(node));
   * }</pre>
   *
   * @param multi the Mutiny Multi source
   * @param <T> type of ObjectNode emitted
   * @return new ObjectNodePublisher
   * @throws IOException if publisher initialization fails
   */
  public static <T extends com.fasterxml.jackson.databind.node.ObjectNode>
      ObjectNodePublisher<T> fromMulti(io.smallrye.mutiny.Multi<ByteBuf> multi)
          throws IOException {
    return createPublisher(multi);
  }

  /**
   * Parses a ByteBuf containing complete JSON and returns all nodes.
   *
   * <p>This is a simple one-shot method for when you have all data in a single ByteBuf.
   * The ByteBuf is released after parsing.
   *
   * <p>Example:
   * <pre>{@code
   * ByteBuf buf = Unpooled.wrappedBuffer(jsonBytes);
   * List<ObjectNode> nodes = ByteBufSupport.parseAll(buf);
   * }</pre>
   *
   * @param buf the ByteBuf containing complete JSON (will be released)
   * @return list of all parsed nodes
   * @throws IOException if parsing fails
   */
  public static java.util.List<com.fasterxml.jackson.databind.node.ObjectNode> parseAll(ByteBuf buf)
      throws IOException {
    try {
      byte[] bytes = new byte[buf.readableBytes()];
      buf.readBytes(bytes);
      return ObjectNodePublisher.parseAll(bytes);
    } finally {
      buf.release();
    }
  }

  /**
   * Subscriber that converts ByteBuf to ByteBuffer with proper reference counting.
   */
  private static class ByteBufToByteBufferSubscriber implements Flow.Subscriber<ByteBuf> {
    private final Flow.Subscriber<? super ByteBuffer> downstream;
    private Flow.Subscription upstream;

    ByteBufToByteBufferSubscriber(Flow.Subscriber<? super ByteBuffer> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      this.upstream = subscription;
      downstream.onSubscribe(new Flow.Subscription() {
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
    public void onNext(ByteBuf buf) {
      try {
        ByteBuffer byteBuffer = toByteBuffer(buf);
        downstream.onNext(byteBuffer);
      } finally {
        // Always release after conversion
        buf.release();
      }
    }

    @Override
    public void onError(Throwable throwable) {
      downstream.onError(throwable);
    }

    @Override
    public void onComplete() {
      downstream.onComplete();
    }

    /**
     * Converts ByteBuf to ByteBuffer with safe copy.
     *
     * <p>Always copies data to ensure the ByteBuffer remains valid after ByteBuf release.
     * This prevents use-after-free issues when downstream processing is slower than
     * upstream production.
     */
    private ByteBuffer toByteBuffer(ByteBuf buf) {
      int readableBytes = buf.readableBytes();
      if (readableBytes == 0) {
        return ByteBuffer.allocate(0);
      }

      // Always copy to ensure memory safety after buf.release()
      // Zero-copy would be unsafe: downstream may hold ByteBuffer reference
      // after ByteBuf is released, causing use-after-free
      byte[] bytes = new byte[readableBytes];
      buf.getBytes(buf.readerIndex(), bytes);
      return ByteBuffer.wrap(bytes);
    }
  }
}
