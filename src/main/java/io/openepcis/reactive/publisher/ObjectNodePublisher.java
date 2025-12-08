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
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A reactive Publisher that streams ObjectNodes from EPCIS JSON documents using non-blocking I/O.
 *
 * <p>This publisher accepts reactive ByteBuffer streams as input, allowing true non-blocking
 * parsing of EPCIS documents from network streams, message queues, etc.
 *
 * <p><strong>Key features:</strong>
 * <ul>
 *   <li>Non-blocking JSON parsing using Jackson's NonBlockingJsonParser</li>
 *   <li>Backpressure support - stops requesting input when subscriber demand is satisfied</li>
 *   <li>Support for early-eventList documents via retry mechanism</li>
 *   <li>Compatible with Multi&lt;ByteBuffer&gt; from Mutiny and other reactive frameworks</li>
 * </ul>
 *
 * <p><strong>Publisher semantics:</strong> This is a <em>cold publisher</em> - each subscription
 * creates independent parser state. Subscribing multiple times results in multiple independent
 * parse operations. Only one subscriber is allowed per publisher instance; attempting to subscribe
 * a second subscriber will result in an error.
 *
 * <p><strong>Backpressure:</strong> Fully supports Reactive Streams backpressure. Input bytes
 * from the source are only requested when downstream subscriber demand exists. When demand is
 * exhausted (subscriber has received all requested items), parsing pauses until more demand
 * arrives via {@link java.util.concurrent.Flow.Subscription#request(long)}. This prevents
 * unbounded memory growth when downstream processing is slower than upstream production.
 *
 * <p><strong>Thread safety:</strong> The publisher itself is thread-safe for subscription.
 * However, the internal parsing state is not thread-safe - all parsing occurs on the thread
 * that drives the source publisher.
 *
 * <p><strong>Early-eventList handling:</strong> Some EPCIS documents have the {@code eventList}
 * array appearing before the {@code @context} field. When this occurs, the parser cannot properly
 * process events on the first pass because the context is needed first. To handle this:
 * <ul>
 *   <li>Without retry: Only the header is emitted, events are skipped</li>
 *   <li>With retry: Provide a {@code retrySource} or {@code retryInputStream} callable that
 *       returns a fresh source. The parser will automatically retry and emit all events.</li>
 * </ul>
 *
 * <p><strong>Usage:</strong>
 * <pre>{@code
 * Flow.Publisher<ByteBuffer> httpBody = ...;
 *
 * ObjectNodePublisher<ObjectNode> publisher = ObjectNodePublisher.builder()
 *     .source(httpBody)
 *     .build();
 *
 * Multi.createFrom().publisher(publisher)
 *     .subscribe().with(node -> process(node));
 * }</pre>
 *
 * @param <T> the type of emitted JSON nodes, typically ObjectNode. This type parameter allows
 *            for subclasses of ObjectNode if needed for specialized processing, though most
 *            use cases will simply use {@code ObjectNodePublisher<ObjectNode>}.
 */
public class ObjectNodePublisher<T extends ObjectNode> implements Publisher<T> {

  private final Flow.Publisher<ByteBuffer> source;
  private final Callable<Flow.Publisher<ByteBuffer>> retrySource;
  private final AtomicReference<ByteBufferSubscription> subscription = new AtomicReference<>();
  private final AtomicLong nodeCount = new AtomicLong();

  /**
   * Creates a new ObjectNodePublisher with a ByteBuffer source.
   *
   * @param source the reactive ByteBuffer source
   * @throws IOException if parser initialization fails
   */
  public ObjectNodePublisher(Flow.Publisher<ByteBuffer> source) throws IOException {
    this(source, null);
  }

  /**
   * Creates a new ObjectNodePublisher with a ByteBuffer source and retry support.
   *
   * @param source the reactive ByteBuffer source
   * @param retrySource callable to get a fresh source for retry (early-eventList handling)
   * @throws IOException if parser initialization fails
   */
  public ObjectNodePublisher(Flow.Publisher<ByteBuffer> source,
                                      Callable<Flow.Publisher<ByteBuffer>> retrySource)
      throws IOException {
    if (source == null) {
      throw new IllegalArgumentException("Source publisher cannot be null");
    }
    this.source = source;
    this.retrySource = retrySource;
  }

  /**
   * Creates a new builder for fluent configuration.
   *
   * @param <T> type of ObjectNode emitted
   * @return new builder instance
   */
  public static <T extends ObjectNode> Builder<T> builder() {
    return new Builder<>();
  }

  /**
   * Creates a publisher from a ByteBuffer source (convenience factory method).
   *
   * <p>Equivalent to {@code new ObjectNodePublisher<>(source)}.
   *
   * @param source the reactive ByteBuffer source
   * @param <T> type of ObjectNode emitted
   * @return new publisher instance
   * @throws IOException if initialization fails
   */
  public static <T extends ObjectNode> ObjectNodePublisher<T> from(
      Flow.Publisher<ByteBuffer> source) throws IOException {
    return new ObjectNodePublisher<>(source);
  }

  /**
   * Creates a publisher from a Mutiny Multi (convenience factory method).
   *
   * <p>Example:
   * <pre>{@code
   * Multi<ByteBuffer> httpBody = ...;
   * ObjectNodePublisher.fromMulti(httpBody)
   *     .toMulti()
   *     .subscribe().with(node -> process(node));
   * }</pre>
   *
   * @param multi the Mutiny Multi source
   * @param <T> type of ObjectNode emitted
   * @return new publisher instance
   * @throws IOException if initialization fails
   */
  public static <T extends ObjectNode> ObjectNodePublisher<T> fromMulti(
      io.smallrye.mutiny.Multi<ByteBuffer> multi) throws IOException {
    return new ObjectNodePublisher<>(multi);
  }

  /**
   * Creates a publisher from a byte array (convenience factory method).
   *
   * <p>Useful for testing or parsing in-memory JSON data.
   *
   * <p>Example:
   * <pre>{@code
   * byte[] json = Files.readAllBytes(path);
   * List<ObjectNode> nodes = ObjectNodePublisher.fromBytes(json)
   *     .toMulti()
   *     .subscribe().asStream().toList();
   * }</pre>
   *
   * @param bytes the JSON data
   * @param <T> type of ObjectNode emitted
   * @return new publisher instance
   * @throws IOException if initialization fails
   */
  public static <T extends ObjectNode> ObjectNodePublisher<T> fromBytes(byte[] bytes)
      throws IOException {
    Flow.Publisher<ByteBuffer> source = subscriber -> {
      subscriber.onSubscribe(new Subscription() {
        private boolean completed = false;

        @Override
        public void request(long n) {
          if (!completed && n > 0) {
            completed = true;
            subscriber.onNext(ByteBuffer.wrap(bytes));
            subscriber.onComplete();
          }
        }

        @Override
        public void cancel() {
          completed = true;
        }
      });
    };
    return new ObjectNodePublisher<>(source);
  }

  /**
   * Creates a publisher from an InputStream (convenience factory method).
   *
   * <p>Reads the InputStream in chunks and feeds them to the async parser.
   * The InputStream is read on-demand as the subscriber requests items.
   *
   * <p><strong>Resource lifecycle:</strong> The caller is responsible for closing the InputStream.
   * This publisher does NOT close the stream on completion or cancellation. Use try-with-resources
   * or ensure proper cleanup in your subscription handlers.
   *
   * <p>Example:
   * <pre>{@code
   * try (InputStream is = new FileInputStream("events.json")) {
   *     List<ObjectNode> nodes = ObjectNodePublisher.fromInputStream(is)
   *         .toMulti()
   *         .subscribe().asStream().toList();
   *     // Process nodes...
   * } // InputStream closed here
   * }</pre>
   *
   * <p>For file-based input with automatic retry support for early-eventList documents,
   * consider using {@link #fromPath(java.nio.file.Path)} instead.
   *
   * @param inputStream the InputStream to read JSON from (caller must close)
   * @param <T> type of ObjectNode emitted
   * @return new publisher instance
   * @throws IOException if initialization fails
   * @see #fromPath(java.nio.file.Path)
   * @see #fromInputStream(java.io.InputStream, Callable)
   */
  public static <T extends ObjectNode> ObjectNodePublisher<T> fromInputStream(
      java.io.InputStream inputStream) throws IOException {
    return fromInputStream(inputStream, 8192);
  }

  /**
   * Creates a publisher from an InputStream with custom buffer size.
   *
   * @param inputStream the InputStream to read JSON from
   * @param bufferSize the size of read buffer (default 8192)
   * @param <T> type of ObjectNode emitted
   * @return new publisher instance
   * @throws IOException if initialization fails
   */
  public static <T extends ObjectNode> ObjectNodePublisher<T> fromInputStream(
      java.io.InputStream inputStream, int bufferSize) throws IOException {
    if (inputStream == null) {
      throw new IllegalArgumentException("InputStream cannot be null");
    }

    Flow.Publisher<ByteBuffer> source = subscriber -> {
      subscriber.onSubscribe(new Subscription() {
        private volatile boolean cancelled = false;
        private final byte[] buffer = new byte[bufferSize];

        @Override
        public void request(long n) {
          if (cancelled) return;

          try {
            for (long i = 0; i < n && !cancelled; i++) {
              int bytesRead = inputStream.read(buffer);
              if (bytesRead == -1) {
                // End of stream
                if (!cancelled) {
                  subscriber.onComplete();
                }
                return;
              }
              // Create a copy of the buffer data for the ByteBuffer
              byte[] chunk = new byte[bytesRead];
              System.arraycopy(buffer, 0, chunk, 0, bytesRead);
              subscriber.onNext(ByteBuffer.wrap(chunk));
            }
          } catch (java.io.IOException e) {
            if (!cancelled) {
              subscriber.onError(e);
            }
          }
        }

        @Override
        public void cancel() {
          cancelled = true;
        }
      });
    };

    return new ObjectNodePublisher<>(source);
  }

  /**
   * Creates a publisher from an InputStream with retry support for early-eventList documents.
   *
   * @param inputStream the primary InputStream
   * @param retryInputStream callable to get a fresh InputStream for retry
   * @param <T> type of ObjectNode emitted
   * @return new publisher instance
   * @throws IOException if initialization fails
   */
  public static <T extends ObjectNode> ObjectNodePublisher<T> fromInputStream(
      java.io.InputStream inputStream,
      Callable<java.io.InputStream> retryInputStream) throws IOException {
    return fromInputStream(inputStream, retryInputStream, 8192);
  }

  /**
   * Creates a publisher from an InputStream with retry support and custom buffer size.
   *
   * @param inputStream the primary InputStream
   * @param retryInputStream callable to get a fresh InputStream for retry
   * @param bufferSize the size of read buffer
   * @param <T> type of ObjectNode emitted
   * @return new publisher instance
   * @throws IOException if initialization fails
   */
  public static <T extends ObjectNode> ObjectNodePublisher<T> fromInputStream(
      java.io.InputStream inputStream,
      Callable<java.io.InputStream> retryInputStream,
      int bufferSize) throws IOException {

    Callable<Flow.Publisher<ByteBuffer>> retrySource = retryInputStream == null ? null : () -> {
      java.io.InputStream retryStream = retryInputStream.call();
      return createInputStreamPublisher(retryStream, bufferSize);
    };

    return new ObjectNodePublisher<>(
        createInputStreamPublisher(inputStream, bufferSize),
        retrySource);
  }

  /**
   * Creates a Flow.Publisher from an InputStream.
   */
  private static Flow.Publisher<ByteBuffer> createInputStreamPublisher(
      java.io.InputStream inputStream, int bufferSize) {
    return subscriber -> {
      subscriber.onSubscribe(new Subscription() {
        private volatile boolean cancelled = false;
        private final byte[] buffer = new byte[bufferSize];

        @Override
        public void request(long n) {
          if (cancelled) return;

          try {
            for (long i = 0; i < n && !cancelled; i++) {
              int bytesRead = inputStream.read(buffer);
              if (bytesRead == -1) {
                if (!cancelled) {
                  subscriber.onComplete();
                }
                return;
              }
              byte[] chunk = new byte[bytesRead];
              System.arraycopy(buffer, 0, chunk, 0, bytesRead);
              subscriber.onNext(ByteBuffer.wrap(chunk));
            }
          } catch (java.io.IOException e) {
            if (!cancelled) {
              subscriber.onError(e);
            }
          }
        }

        @Override
        public void cancel() {
          cancelled = true;
        }
      });
    };
  }

  /**
   * Parses JSON bytes and returns all nodes as a list (blocking convenience method).
   *
   * <p>This is a simple one-shot method for when you don't need streaming.
   *
   * <p>Example:
   * <pre>{@code
   * byte[] json = ...;
   * List<ObjectNode> nodes = ObjectNodePublisher.parseAll(json);
   * ObjectNode header = nodes.get(0);
   * List<ObjectNode> events = nodes.subList(1, nodes.size());
   * }</pre>
   *
   * @param bytes the JSON data
   * @return list of all parsed nodes (header first, then events)
   * @throws IOException if parsing fails
   */
  public static java.util.List<ObjectNode> parseAll(byte[] bytes) throws IOException {
    return fromBytes(bytes).toMulti().subscribe().asStream().toList();
  }

  /**
   * Creates a publisher from a file path with automatic retry support.
   *
   * <p>This method handles the early-eventList problem automatically by providing a retry
   * mechanism that reopens the file if needed. This is the recommended way to parse EPCIS
   * documents from files.
   *
   * <p>Example:
   * <pre>{@code
   * Path jsonFile = Path.of("events.json");
   * ObjectNodePublisher.fromPath(jsonFile)
   *     .toMulti()
   *     .subscribe().with(node -> process(node));
   * }</pre>
   *
   * @param path the path to the JSON file
   * @param <T> type of ObjectNode emitted
   * @return new publisher instance
   * @throws IOException if file cannot be opened
   */
  public static <T extends ObjectNode> ObjectNodePublisher<T> fromPath(java.nio.file.Path path)
      throws IOException {
    return fromInputStream(
        java.nio.file.Files.newInputStream(path),
        () -> java.nio.file.Files.newInputStream(path));
  }

  /**
   * Converts this publisher to a Mutiny Multi for easier reactive composition.
   *
   * <p>Example:
   * <pre>{@code
   * publisher.toMulti()
   *     .filter(node -> "ObjectEvent".equals(node.path("type").asText()))
   *     .onItem().transform(this::processEvent)
   *     .subscribe().with(result -> log.info("Processed: {}", result));
   * }</pre>
   *
   * @return Multi wrapping this publisher
   */
  public io.smallrye.mutiny.Multi<T> toMulti() {
    return io.smallrye.mutiny.Multi.createFrom().publisher(this);
  }

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    if (subscriber == null) {
      throw new NullPointerException("Subscriber cannot be null");
    }

    ByteBufferSubscription sub = new ByteBufferSubscription(subscriber);
    if (!subscription.compareAndSet(null, sub)) {
      subscriber.onSubscribe(new Subscription() {
        @Override
        public void request(long n) {}
        @Override
        public void cancel() {}
      });
      subscriber.onError(new IllegalStateException("Publisher already has a subscriber"));
      return;
    }

    subscriber.onSubscribe(sub);
  }

  /**
   * Returns the total number of nodes emitted so far.
   *
   * @return node count
   */
  public long getNodeCount() {
    return nodeCount.get();
  }

  /**
   * Subscription implementation that bridges ByteBuffer source to ObjectNode output.
   */
  private class ByteBufferSubscription implements Subscription {
    private volatile Subscriber<? super T> subscriber;
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final AtomicLong demand = new AtomicLong(0);
    private final AtomicReference<Throwable> error = new AtomicReference<>();

    // Recursion guard to prevent unbounded recursion (spec303)
    private final AtomicBoolean emitting = new AtomicBoolean(false);
    // Flag to indicate work was missed due to recursion guard
    private volatile boolean missedWork = false;

    private AsyncObjectNodeParser parser;
    private volatile Flow.Subscription sourceSubscription;
    private volatile boolean parserInitialized = false;
    private volatile boolean sourceComplete = false;  // Track if source has completed
    private boolean secondPass = false;

    ByteBufferSubscription(Subscriber<? super T> subscriber) {
      this.subscriber = subscriber;
    }

    @Override
    public void request(long n) {
      if (n <= 0) {
        Subscriber<? super T> s = subscriber;
        cancel();
        if (s != null) {
          s.onError(new IllegalArgumentException("Non-positive subscription request: " + n));
        }
        return;
      }

      demand.addAndGet(n);

      // Initialize parser and subscribe to source on first request
      if (!parserInitialized) {
        try {
          parser = new AsyncObjectNodeParser();
          parserInitialized = true;
          subscribeToSource(source);
        } catch (IOException e) {
          Subscriber<? super T> s = subscriber;
          cancel();
          if (s != null) {
            s.onError(e);
          }
          return;
        }
      }

      // Try to emit any ready nodes (with recursion guard)
      drain();
    }

    @Override
    public void cancel() {
      if (terminated.getAndSet(true)) {
        return;
      }

      // Drop subscriber reference per spec313
      subscriber = null;

      if (sourceSubscription != null) {
        sourceSubscription.cancel();
        sourceSubscription = null;
      }

      if (parser != null) {
        parser.close();
        parser = null;
      }
    }

    /**
     * Subscribes to the ByteBuffer source.
     */
    private void subscribeToSource(Flow.Publisher<ByteBuffer> src) {
      src.subscribe(new Flow.Subscriber<>() {
        @Override
        public void onSubscribe(Flow.Subscription s) {
          sourceSubscription = s;
          // Request first chunk
          s.request(1);
        }

        @Override
        public void onNext(ByteBuffer buffer) {
          if (terminated.get()) {
            return;
          }

          try {
            AsyncObjectNodeParser p = parser;
            if (p == null) {
              return;
            }

            p.feedInput(buffer);

            // Signal that work is available - drain will handle emission
            drain();

          } catch (IOException e) {
            Subscriber<? super T> s = subscriber;
            error.set(e);
            cancel();
            if (s != null) {
              s.onError(e);
            }
          }
        }

        @Override
        public void onError(Throwable t) {
          Subscriber<? super T> s = subscriber;
          if (!terminated.getAndSet(true)) {
            error.set(t);
            // Drop references per spec313
            subscriber = null;
            if (s != null) {
              s.onError(t);
            }
          }
        }

        @Override
        public void onComplete() {
          if (terminated.get()) {
            return;
          }

          sourceComplete = true;

          try {
            AsyncObjectNodeParser p = parser;
            if (p == null) {
              return;
            }

            p.endOfInput();

            // Check if we need retry for early-eventList
            if (p.isIgnoreEventList() && retrySource != null && !secondPass) {
              secondPass = true;
              sourceComplete = false;  // Reset for retry
              AsyncObjectNodeParser oldParser = parser;
              try {
                parser = new AsyncObjectNodeParser();
                subscribeToSource(retrySource.call());
              } finally {
                if (oldParser != null) {
                  oldParser.close();
                }
              }
              return;
            }

            // Drain any remaining nodes and complete
            drain();

          } catch (IOException e) {
            Subscriber<? super T> s = subscriber;
            if (!terminated.getAndSet(true)) {
              subscriber = null;
              if (s != null) {
                s.onError(new IllegalStateException(
                    "EPCIS parsing failed" + (secondPass ? " during early-eventList retry" : ""), e));
              }
            }
          } catch (Exception e) {
            Subscriber<? super T> s = subscriber;
            if (!terminated.getAndSet(true)) {
              subscriber = null;
              if (s != null) {
                s.onError(new IllegalStateException(
                    "Unexpected error during EPCIS processing", e));
              }
            }
          }
        }
      });
    }

    /**
     * Non-recursive drain method that emits nodes using a guard flag (spec303).
     */
    private void drain() {
      // Recursion guard - if we're already emitting, signal missed work and return
      if (!emitting.compareAndSet(false, true)) {
        missedWork = true;
        return;
      }

      try {
        do {
          missedWork = false;
          emitReadyNodes();
        } while (missedWork && !terminated.get());
      } finally {
        emitting.set(false);
      }

      // If work was flagged while clearing emitting, re-enter
      if (missedWork && !terminated.get()) {
        drain();
      }
    }

    /**
     * Emits all ready nodes up to the current demand.
     */
    @SuppressWarnings("unchecked")
    private void emitReadyNodes() {
      Subscriber<? super T> s = subscriber;
      AsyncObjectNodeParser p = parser;

      if (terminated.get() || s == null || p == null) {
        return;
      }

      // Emit all available nodes up to demand
      ObjectNode node;
      while (demand.get() > 0 && (node = p.pollNextNode()) != null) {
        if (terminated.get()) {
          return;
        }
        demand.decrementAndGet();
        nodeCount.incrementAndGet();
        s.onNext((T) node);
      }

      // If we still have demand and source hasn't completed, request more input
      Flow.Subscription src = sourceSubscription;
      if (demand.get() > 0 && !sourceComplete && src != null && !terminated.get()) {
        src.request(1);
      }

      // If source is complete and no more nodes, complete the stream
      if (sourceComplete && !p.hasMoreNodes() && !terminated.get()) {
        Subscriber<? super T> sub = subscriber;
        if (!terminated.getAndSet(true) && sub != null) {
          subscriber = null;
          sub.onComplete();
        }
      }
    }
  }

  /**
   * Builder for ObjectNodePublisher.
   *
   * <p>Supports building from either a reactive ByteBuffer source or an InputStream.
   *
   * <p><strong>Example with InputStream:</strong>
   * <pre>{@code
   * ObjectNodePublisher<ObjectNode> publisher = ObjectNodePublisher.builder()
   *     .inputStream(myInputStream)
   *     .retryInputStream(() -> new FileInputStream("data.json"))
   *     .build();
   * }</pre>
   *
   * <p><strong>Example with reactive source:</strong>
   * <pre>{@code
   * ObjectNodePublisher<ObjectNode> publisher = ObjectNodePublisher.builder()
   *     .source(byteBufferPublisher)
   *     .retrySource(() -> createNewPublisher())
   *     .build();
   * }</pre>
   *
   * @param <T> type of ObjectNode emitted
   */
  public static class Builder<T extends ObjectNode> {
    private static final int DEFAULT_BUFFER_SIZE = 8192;

    private Flow.Publisher<ByteBuffer> source;
    private Callable<Flow.Publisher<ByteBuffer>> retrySource;
    private java.io.InputStream inputStream;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private Callable<java.io.InputStream> retryInputStream;

    Builder() {}

    /**
     * Sets the ByteBuffer source publisher.
     *
     * <p>Cannot be combined with {@link #inputStream(java.io.InputStream)}.
     *
     * @param source the reactive ByteBuffer source
     * @return this builder
     */
    public Builder<T> source(Flow.Publisher<ByteBuffer> source) {
      this.source = source;
      return this;
    }

    /**
     * Sets the retry source callable for early-eventList handling.
     *
     * <p>Use with {@link #source(Flow.Publisher)}.
     *
     * @param retrySource callable that provides a fresh source
     * @return this builder
     */
    public Builder<T> retrySource(Callable<Flow.Publisher<ByteBuffer>> retrySource) {
      this.retrySource = retrySource;
      return this;
    }

    /**
     * Sets the InputStream to parse.
     *
     * <p>Cannot be combined with {@link #source(Flow.Publisher)}.
     *
     * @param inputStream the InputStream to read JSON from
     * @return this builder
     */
    public Builder<T> inputStream(java.io.InputStream inputStream) {
      this.inputStream = inputStream;
      return this;
    }

    /**
     * Sets the buffer size for reading from InputStream.
     *
     * <p>Only applicable when using {@link #inputStream(java.io.InputStream)}.
     * Default is 8192 bytes.
     *
     * @param bufferSize the buffer size in bytes
     * @return this builder
     */
    public Builder<T> bufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    /**
     * Sets the retry InputStream callable for early-eventList handling.
     *
     * <p>Use with {@link #inputStream(java.io.InputStream)}.
     *
     * @param retryInputStream callable that provides a fresh InputStream
     * @return this builder
     */
    public Builder<T> retryInputStream(Callable<java.io.InputStream> retryInputStream) {
      this.retryInputStream = retryInputStream;
      return this;
    }

    /**
     * Builds the ObjectNodePublisher.
     *
     * @return new publisher instance
     * @throws IOException if initialization fails
     * @throws IllegalStateException if neither source nor inputStream is set, or if both are set
     */
    @SuppressWarnings("unchecked")
    public ObjectNodePublisher<T> build() throws IOException {
      // Validate configuration
      if (source != null && inputStream != null) {
        throw new IllegalStateException("Cannot set both source and inputStream");
      }
      if (source == null && inputStream == null) {
        throw new IllegalStateException("Either source or inputStream must be set");
      }
      if (retrySource != null && retryInputStream != null) {
        throw new IllegalStateException("Cannot set both retrySource and retryInputStream");
      }
      if (source != null && retryInputStream != null) {
        throw new IllegalStateException("retryInputStream can only be used with inputStream, not source");
      }
      if (inputStream != null && retrySource != null) {
        throw new IllegalStateException("retrySource can only be used with source, not inputStream");
      }

      // Build from source
      if (source != null) {
        return (ObjectNodePublisher<T>) new ObjectNodePublisher<>(source, retrySource);
      }

      // Build from InputStream
      Callable<Flow.Publisher<ByteBuffer>> retryPublisher = retryInputStream == null ? null : () -> {
        java.io.InputStream retryStream = retryInputStream.call();
        return createInputStreamPublisher(retryStream, bufferSize);
      };

      return (ObjectNodePublisher<T>) new ObjectNodePublisher<>(
          createInputStreamPublisher(inputStream, bufferSize),
          retryPublisher);
    }
  }
}
