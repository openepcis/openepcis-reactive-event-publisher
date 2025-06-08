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

import static io.openepcis.constants.EPCIS.*;
import static io.openepcis.constants.EPCIS.RESULTS_BODY_IN_CAMEL_CASE;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * A reactive Publisher that streams ObjectNodes parsed from an EPCIS JSON document.
 * Supports partial parsing, retry capability, and backpressure-aware consumption.
 *
 * @param <T> the type of emitted JSON nodes, typically ObjectNode
 */
@Slf4j
public class ObjectNodePublisher<T extends ObjectNode> implements Publisher<T> {
  private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
  private static final JsonFactory jsonFactory = new JsonFactory();
  private final ObjectNode header = mapper.createObjectNode();
  private JsonParser jsonParser;
  private final AtomicBoolean headerSent = new AtomicBoolean(false);
  private final AtomicBoolean inEventList = new AtomicBoolean(false);
  private final AtomicBoolean ignoreEventList = new AtomicBoolean(false);
  private final AtomicLong nodeCount = new AtomicLong();
  private final AtomicReference<ObjectNodeSubscription> subscription = new AtomicReference<>();
  private JsonToken token;
  final Callable<InputStream> retryInputStream;
  final Callable<Reader> retryReader;

  /**
   * Constructs an ObjectNodePublisher with an InputStream and retry support.
   *
   * @param in    the primary InputStream for parsing
   * @param retry the retryable InputStream callable for second pass
   * @throws IOException if JSON parser cannot be initialized
   */
  public ObjectNodePublisher(final InputStream in, final Callable<InputStream> retry) throws IOException {
    this.jsonParser = jsonFactory.createParser(in);
    this.jsonParser.setCodec(mapper);
    this.retryInputStream = retry;
    this.retryReader = null;
  }

  /**
   * Constructs an ObjectNodePublisher with a one-shot InputStream.
   *
   * @param in InputStream for reading the JSON content
   * @throws IOException if parser initialization fails
   */
  public ObjectNodePublisher(final InputStream in) throws IOException {
    this(in, null);
  }

  /**
   * Constructs an ObjectNodePublisher with a Reader and retry fallback.
   *
   * @param reader primary Reader
   * @param retry  optional retryable Reader
   * @throws IOException if JSON parser cannot be initialized
   */
  public ObjectNodePublisher(final Reader reader, final Callable<Reader> retry) throws IOException {
    this.jsonParser = jsonFactory.createParser(reader);
    this.jsonParser.setCodec(mapper);
    this.retryInputStream = null;
    this.retryReader = retry;
  }

  /**
   * Constructs an ObjectNodePublisher with a one-shot Reader.
   *
   * @param reader JSON document reader
   * @throws IOException if initialization fails
   */
  public ObjectNodePublisher(final Reader reader) throws IOException {
    this(reader, null);
  }

  /**
   * Subscribes a reactive Subscriber to this publisher.
   * Emits a valid header (if present), followed by event nodes.
   *
   * @param subscriber the downstream subscriber
   */
  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    this.subscription.set(new ObjectNodeSubscription(subscriber));
    final Optional<Throwable> throwable = beginParsing(this.jsonParser);
    throwable.ifPresent(this.subscription.get()::error);
    subscriber.onSubscribe(this.subscription.get());
    throwable.ifPresent(subscriber::onError);
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
   * Initializes the JSON parser and processes header fields.
   * Detects and prepares for eventList parsing.
   *
   * @param jsonParser the parser instance
   * @return an Optional error, if any
   */
  private Optional<Throwable> beginParsing(final JsonParser jsonParser) {
    try {
      jsonParser.setCodec(mapper);
      token = jsonParser.nextToken();
      while (token != null && token != JsonToken.END_OBJECT) {
        final String fieldName = jsonParser.nextFieldName();
        token = jsonParser.nextToken();
        if (fieldName != null && fieldName.equals(EVENT_LIST_IN_CAMEL_CASE)) {
          if (token != JsonToken.START_ARRAY) {
            return Optional.of(new IOException("invalid eventList structure, must be an array"));
          }
          token = jsonParser.nextToken();
          inEventList.set(true);
          return Optional.empty();
        } else if (fieldName != null && !fieldName.equals(EPCIS_BODY_IN_CAMEL_CASE)
                && !fieldName.equals(QUERY_RESULTS_IN_CAMEL_CASE)
                && !fieldName.equals(RESULTS_BODY_IN_CAMEL_CASE)
                && !headerSent.get()) {
          final JsonNode o = jsonParser.readValueAsTree();
          if (o != null) {
            header.set(fieldName, o);
          }
        }
      }
      if (token == null) {
        jsonParser.close();
      }
    } catch (Exception e) {
      log.error("Error while parsing epcis document", e);
      return Optional.of(e);
    }
    return Optional.empty();
  }

  /**
   * Instructs the publisher to ignore the eventList section.
   *
   * @param ignore true to skip event list parsing
   */
  public void setIgnoreEventList(boolean ignore) {
    ignoreEventList.set(ignore);
  }

  /**
   * Checks whether the event list section is skipped.
   *
   * @return true if skipped
   */
  public boolean isEventListIgnored() {
    return ignoreEventList.get();
  }

  /**
   * Subscription implementation for managing demand and streaming of ObjectNodes.
   * Supports backpressure, event list parsing, and retry logic if needed.
   */
  public class ObjectNodeSubscription implements Subscription {
    /**
     * Flag to indicate if the subscription is terminated.
     */
    private final AtomicBoolean isTerminated = new AtomicBoolean(false);

    /**
     * Tracks the number of items requested but not yet delivered.
     */
    private final AtomicLong demand = new AtomicLong();

    /**
     * The subscriber associated with this subscription.
     */
    private final AtomicReference<Subscriber<? super T>> subscriber;

    /**
     * Holds any error encountered during parsing or processing.
     */
    private final AtomicReference<Throwable> throwable = new AtomicReference<>();

    /**
     * Flag to control whether a retry attempt has already been made.
     */
    private boolean secondPass = false;

    /**
     * Constructs a new subscription for the given subscriber.
     *
     * @param subscriber the reactive subscriber
     * @throws NullPointerException if the subscriber is null
     */
    private ObjectNodeSubscription(Subscriber<? super T> subscriber) {
      if (subscriber == null) throw new NullPointerException("subscriber must not be null");
      this.subscriber = new AtomicReference<>(subscriber);
    }

    /**
     * Requests the given number of items from the publisher.
     * Handles streaming of parsed ObjectNodes with support for retry if needed.
     *
     * @param l the number of items requested; must be greater than 0
     */
    @Override
    public void request(long l) {
      if (l <= 0 && !terminate()) {
        subscriber.get().onError(new IllegalArgumentException("negative subscription request"));
        return;
      }
      if (hasError() && !terminate()) {
        subscriber.get().onError(throwable.get());
        return;
      }
      if (demand.get() > 0) {
        demand.getAndAdd(l);
        return;
      }
      demand.getAndAdd(l);
      try {
        while (demand.get() > 0 && !isTerminated() && !hasError()) {
          final long count = readNext(demand.get());
          if (count >= 0) {
            demand.getAndAdd(-1 * count);
            nodeCount.getAndAdd(count);
          } else if (!terminate()) {
            if (!secondPass && (retryInputStream != null || retryReader != null) && ignoreEventList.get()) {
              secondPass = true;
              if (!jsonParser.isClosed()) consumeAndClose(jsonParser);
              Optional<Throwable> throwable = Optional.empty();
              if (retryInputStream != null) {
                jsonParser = jsonFactory.createParser(retryInputStream.call());
                jsonParser.setCodec(mapper);
                throwable = beginParsing(jsonParser);
              }
              if (retryReader != null) {
                jsonParser = jsonFactory.createParser(retryReader.call());
                jsonParser.setCodec(mapper);
                throwable = beginParsing(jsonParser);
              }
              if (throwable.isPresent()) {
                subscription.get().error(throwable.get());
                subscriber.get().onError(throwable.get());
              } else {
                ignoreEventList.set(false);
                isTerminated.set(false);
                continue;
              }
            }
            subscriber.get().onComplete();
            return;
          }
        }
      } catch (Exception ex) {
        if (!terminate()) subscriber.get().onError(ex);
      }
    }

    /**
     * Consumes any remaining tokens from the parser and closes it.
     *
     * @param parser the JSON parser to close
     */
    public void consumeAndClose(JsonParser parser) {
      if (parser == null) return;
      try {
        while (parser.nextToken() != null) {
          parser.skipChildren();
        }
      } catch (IOException ignored) {
      } finally {
        try {
          parser.close();
        } catch (IOException ignored) {
        }
      }
    }

    /**
     * Cancels the subscription and nullifies the subscriber reference.
     */
    @Override
    public void cancel() {
      terminate();
      subscriber.set(null);
    }

    /**
     * Marks the subscription as terminated.
     *
     * @return true if this call marked it as terminated; false if it was already terminated
     */
    private boolean terminate() {
      return isTerminated.getAndSet(true);
    }

    /**
     * Checks whether the subscription has been terminated.
     *
     * @return true if the subscription is terminated
     */
    private boolean isTerminated() {
      return isTerminated.get();
    }

    /**
     * Stores the provided error and returns the previously stored one.
     *
     * @param throwable the error to store
     * @return the previously stored error, or null
     */
    private Throwable error(final Throwable throwable) {
      return this.throwable.getAndSet(throwable);
    }

    /**
     * Checks whether an error has been recorded.
     *
     * @return true if an error is stored
     */
    private boolean hasError() {
      return throwable.get() != null;
    }

    /**
     * Attempts to read and emit up to {@code requested} number of ObjectNodes.
     * Includes logic for header emission, event list processing, and EOF handling.
     *
     * @param requested the maximum number of nodes to read
     * @return the number of nodes actually read, or -1 if none could be read
     * @throws IOException if a parsing error occurs
     */
    private long readNext(final long requested) throws IOException {
      long l = publishValidHeaderNode(requested);
      if ((requested - l) > 0 && inEventList.get() && !headerSent.get()) {
        ignoreEventList.set(true);
      }
      l += readEventList(requested - l);
      l += processEOF(requested - l);
      return l > 0 || isTokenAvailable() ? l : -1;
    }

    /**
     * Reads ObjectNodes from the eventList array.
     * Skips ignored sections and emits only valid EPCIS events.
     *
     * @param requested the maximum number of events to emit
     * @return the number of events emitted
     * @throws IOException if parsing fails
     */
    private long readEventList(final long requested) throws IOException {
      if (!inEventList.get() || requested == 0) return 0;
      if (isEventListIgnored() && isTokenAvailable()) {
        int array = 0;
        while (isEventListIgnored() && isTokenAvailable()) {
          if (token == JsonToken.START_ARRAY) {
            array++;
          }
          if (array == 0 && token == JsonToken.END_ARRAY) return 0;
          if (token == JsonToken.END_ARRAY) {
            array--;
          }
          token = jsonParser.nextToken();
        }
      }
      long l = 0;
      while (!isEventListIgnored() && isTokenAvailable() && token == JsonToken.START_OBJECT && l < requested) {
        JsonNode o = jsonParser.readValueAsTree();
        if (o.has(TYPE)) {
          l++;
          subscriber.get().onNext((T) o);
        }
        token = jsonParser.nextToken();
      }
      if (token == JsonToken.END_ARRAY) {
        inEventList.set(false);
        jsonParser.nextToken();
        token = jsonParser.nextToken();
      }
      return l;
    }

    /**
     * Emits the header ObjectNode if it has not yet been sent and is valid.
     *
     * @param requested the current request count
     * @return 1 if the header was emitted; 0 otherwise
     */
    private long publishValidHeaderNode(final long requested) {
      if (requested > 0 && !headerSent.get()
              && ((!isTokenAvailable() && ObjectNodeUtil.isValidEPCISDocumentNode(header))
              || (isTokenAvailable() && nodeCount.get() == 0 && ObjectNodeUtil.isValidEPCISDocumentNode(header)))) {
        // move epcisBody to the end
        if (header.has(EPCIS_BODY_IN_CAMEL_CASE)) {
          header.remove(EPCIS_BODY_IN_CAMEL_CASE);
          header.set(EPCIS_BODY_IN_CAMEL_CASE, NullNode.getInstance());
        }
        headerSent.set(true);
        subscriber.get().onNext((T) header);
        return 1;
      }
      return 0;
    }

    /**
     * Handles any trailing tokens and attempts to emit the header at EOF.
     *
     * @param requested number of requested items
     * @return number of nodes emitted as a result of EOF handling
     * @throws IOException if reading fails
     */
    private synchronized long processEOF(final long requested) throws IOException {
      if (requested == 0) return 0;
      if (isTokenAvailable() && (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY || token == JsonToken.FIELD_NAME)) {
        appendHeaderFields();
        token = jsonParser.nextToken();
      }
      return publishValidHeaderNode(requested);
    }

    /**
     * Appends any remaining JSON fields to the header node before closing.
     *
     * @throws IOException if token parsing fails
     */
    private void appendHeaderFields() throws IOException {
      while (isTokenAvailable() && token != JsonToken.END_OBJECT) {
        if (!headerSent.get()) {
          final String fieldName = jsonParser.nextFieldName();
          final JsonNode j = jsonParser.readValueAsTree();
          if (j != null) {
            header.set(fieldName != null ? fieldName : jsonParser.getCurrentName(), j);
          }
        }
        token = jsonParser.nextToken();
      }
    }

    /**
     * Indicates whether the JSON parser has more tokens to read.
     *
     * @return true if the parser is still open and has tokens remaining
     */
    private boolean isTokenAvailable() {
      return !jsonParser.isClosed();
    }
  }
}