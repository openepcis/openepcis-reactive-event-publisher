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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.async.ByteArrayFeeder;
import com.fasterxml.jackson.core.json.async.NonBlockingJsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Async JSON parser for EPCIS documents using Jackson's NonBlockingJsonParser.
 *
 * <p>This parser builds ObjectNode trees from JSON tokens without blocking. It's designed
 * specifically for EPCIS document structure:
 * <ul>
 *   <li>Emits the document header first (with @context, type, schemaVersion, etc.)</li>
 *   <li>Emits each event from eventList as a separate ObjectNode</li>
 *   <li>Automatically detects and handles early-eventList documents (eventList before @context)</li>
 * </ul>
 *
 * <h2>Early-eventList Handling</h2>
 *
 * <p>Some EPCIS documents have the {@code eventList} appearing before the {@code @context} field.
 * This parser automatically detects this situation:
 *
 * <ul>
 *   <li><strong>Detection:</strong> When entering eventList, if @context hasn't been seen yet,
 *       {@link #isEarlyEventListDetected()} returns true</li>
 *   <li><strong>First pass behavior:</strong> Events are skipped, only header is emitted</li>
 *   <li><strong>Retry pass:</strong> Use {@link #setRetryPass()} before parsing to process events
 *       (header will be skipped since it was emitted on first pass)</li>
 * </ul>
 *
 * <p>This design enables {@link ObjectNodePublisher} to automatically retry with a fresh source
 * when early-eventList is detected, ensuring all events are properly parsed.
 *
 * <p><strong>Usage:</strong>
 * <pre>{@code
 * AsyncObjectNodeParser parser = new AsyncObjectNodeParser();
 *
 * // Feed bytes as they arrive
 * FeedResult result = parser.feedInput(bytes, 0, bytes.length);
 *
 * // Poll for completed nodes
 * ObjectNode node;
 * while ((node = parser.pollNextNode()) != null) {
 *     // Process node
 * }
 *
 * // When input is complete
 * parser.endOfInput();
 *
 * // Check if retry is needed
 * if (parser.isEarlyEventListDetected()) {
 *     // Need to retry with fresh source - events were skipped
 * }
 * }</pre>
 *
 * <p><strong>Thread safety:</strong> This class is NOT thread-safe. Access from single thread only.
 *
 * @see ObjectNodePublisher
 * @see #isEarlyEventListDetected()
 * @see #setRetryPass()
 */
public class AsyncObjectNodeParser {

  private static final System.Logger log = System.getLogger(AsyncObjectNodeParser.class.getName());

  /** Result of feeding input to the parser */
  public enum FeedResult {
    /** One or more ObjectNodes are ready to poll */
    NODES_AVAILABLE,
    /** Parser needs more input data */
    NEED_MORE_INPUT,
    /** End of input reached, no more nodes */
    COMPLETE
  }

  /** Parser state */
  private enum State {
    /** Initial state, waiting for document start */
    INITIAL,
    /** Reading document header fields */
    READING_HEADER,
    /** Inside epcisBody, looking for eventList */
    IN_EPCIS_BODY,
    /** Inside eventList array, parsing events */
    IN_EVENT_LIST,
    /** Parsing an individual event object */
    PARSING_EVENT,
    /** Document complete */
    COMPLETE,
    /** Error state */
    ERROR
  }

  private static final JsonFactory JSON_FACTORY = new JsonFactory();
  private static final JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

  /** Maximum allowed nesting depth to prevent StackOverflowError on deeply nested JSON */
  private static final int MAX_NESTING_DEPTH = 1000;

  private final NonBlockingJsonParser parser;
  private final ByteArrayFeeder feeder;
  private final Queue<ObjectNode> readyNodes = new ConcurrentLinkedQueue<>();

  // Parser state
  private State state = State.INITIAL;
  private boolean inputComplete = false;
  private boolean headerEmitted = false;
  private boolean earlyEventListDetected = false;
  private boolean retryPass = false;

  // Tree building state
  private final Deque<ContainerNode<?>> nodeStack = new ArrayDeque<>();
  private String currentFieldName = null;

  // Header accumulation
  private ObjectNode header = NODE_FACTORY.objectNode();

  // Event building
  private ObjectNode currentEvent = null;
  private int eventDepth = 0;

  // Tracking nested depth in epcisBody/eventList
  private int epcisBodyDepth = 0;
  private int eventListArrayDepth = 0;

  /**
   * Creates a new async EPCIS parser.
   *
   * @throws IOException if parser initialization fails
   */
  public AsyncObjectNodeParser() throws IOException {
    this.parser = (NonBlockingJsonParser) JSON_FACTORY.createNonBlockingByteArrayParser();
    this.feeder = parser.getNonBlockingInputFeeder();
  }

  /**
   * Feeds input bytes to the parser.
   *
   * @param data byte array containing JSON data
   * @param offset start offset in array
   * @param length number of bytes to feed
   * @return result indicating parser state after feeding
   * @throws IOException if parsing error occurs
   */
  public FeedResult feedInput(byte[] data, int offset, int length) throws IOException {
    if (state == State.COMPLETE || state == State.ERROR) {
      return FeedResult.COMPLETE;
    }

    feeder.feedInput(data, offset, offset + length);
    return processTokens();
  }

  /**
   * Feeds input from a ByteBuffer to the parser.
   *
   * @param buffer ByteBuffer containing JSON data
   * @return result indicating parser state after feeding
   * @throws IOException if parsing error occurs
   */
  public FeedResult feedInput(ByteBuffer buffer) throws IOException {
    if (state == State.COMPLETE || state == State.ERROR) {
      return FeedResult.COMPLETE;
    }

    if (buffer.hasArray()) {
      // Zero-copy for heap buffers
      feeder.feedInput(buffer.array(),
          buffer.arrayOffset() + buffer.position(),
          buffer.arrayOffset() + buffer.limit());
      buffer.position(buffer.limit());
    } else {
      // Copy for direct buffers
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      feeder.feedInput(bytes, 0, bytes.length);
    }

    return processTokens();
  }

  /**
   * Signals that all input has been provided.
   *
   * @throws IOException if parsing error occurs
   */
  public void endOfInput() throws IOException {
    inputComplete = true;
    feeder.endOfInput();
    processTokens();

    // Emit header if we have a valid one and haven't emitted it
    // On retry pass, skip header (already emitted on first pass)
    if (!headerEmitted && !retryPass && ObjectNodeUtil.isValidEPCISDocumentNode(header)) {
      emitHeader();
    }

    state = State.COMPLETE;
  }

  /**
   * Polls for the next completed ObjectNode.
   *
   * @return next node, or null if none available
   */
  public ObjectNode pollNextNode() {
    return readyNodes.poll();
  }

  /**
   * Checks if there are nodes ready to poll.
   *
   * @return true if nodes are available
   */
  public boolean hasMoreNodes() {
    return !readyNodes.isEmpty();
  }

  /**
   * Checks if the parser needs more input.
   *
   * @return true if more input is needed
   */
  public boolean needsMoreInput() {
    return feeder.needMoreInput() && !inputComplete && state != State.COMPLETE;
  }

  /**
   * Checks if parsing is complete.
   *
   * @return true if complete
   */
  public boolean isComplete() {
    return state == State.COMPLETE;
  }

  /**
   * Checks if early-eventList was detected (eventList before @context).
   * When true, retry with a fresh source is needed to properly parse events.
   *
   * @return true if early-eventList was detected
   */
  public boolean isEarlyEventListDetected() {
    return earlyEventListDetected;
  }

  /**
   * Marks this parser as a retry pass. On retry, early-eventList detection
   * is disabled so events are processed normally.
   */
  public void setRetryPass() {
    this.retryPass = true;
  }

  /**
   * Processes available tokens from the parser.
   */
  private FeedResult processTokens() throws IOException {
    JsonToken token;

    while ((token = parser.nextToken()) != JsonToken.NOT_AVAILABLE) {
      if (token == null) {
        // End of input
        break;
      }

      // Modern Java switch expression for state dispatch
      switch (state) {
        case INITIAL -> handleInitialState(token);
        case READING_HEADER -> handleReadingHeader(token);
        case IN_EPCIS_BODY -> handleInEpcisBody(token);
        case IN_EVENT_LIST -> handleInEventList(token);
        case PARSING_EVENT -> handleParsingEvent(token);
        case COMPLETE, ERROR -> {
          return FeedResult.COMPLETE;
        }
      }
    }

    if (!readyNodes.isEmpty()) {
      return FeedResult.NODES_AVAILABLE;
    }

    if (state == State.COMPLETE) {
      return FeedResult.COMPLETE;
    }

    return FeedResult.NEED_MORE_INPUT;
  }

  /**
   * Handles tokens in INITIAL state.
   */
  private void handleInitialState(JsonToken token) throws IOException {
    if (token == JsonToken.START_OBJECT) {
      state = State.READING_HEADER;
      header = NODE_FACTORY.objectNode();
    } else {
      throw new IOException("Expected START_OBJECT at document root, got: " + token);
    }
  }

  /**
   * Handles tokens while reading document header.
   */
  private void handleReadingHeader(JsonToken token) throws IOException {
    switch (token) {
      case FIELD_NAME:
        currentFieldName = parser.currentName();

        if (EPCIS_BODY_IN_CAMEL_CASE.equals(currentFieldName)) {
          state = State.IN_EPCIS_BODY;
          epcisBodyDepth = 0;
        } else if (QUERY_RESULTS_IN_CAMEL_CASE.equals(currentFieldName)
            || RESULTS_BODY_IN_CAMEL_CASE.equals(currentFieldName)) {
          // Skip query response wrappers
          state = State.IN_EPCIS_BODY;
          epcisBodyDepth = 0;
        }
        break;

      case START_OBJECT:
        // Nested object in header
        ObjectNode nestedObj = NODE_FACTORY.objectNode();
        header.set(currentFieldName, nestedObj);
        nodeStack.push(header);
        header = nestedObj;
        break;

      case START_ARRAY:
        // Array in header (like @context)
        ArrayNode arr = NODE_FACTORY.arrayNode();
        header.set(currentFieldName, arr);
        nodeStack.push(header);
        readArrayIntoParent(arr, 0);
        break;

      case END_OBJECT:
        if (nodeStack.isEmpty()) {
          // Document root closed - emit header if valid
          // On retry pass, skip header (already emitted on first pass)
          if (!headerEmitted && !retryPass && ObjectNodeUtil.isValidEPCISDocumentNode(header)) {
            emitHeader();
          }
          state = State.COMPLETE;
        } else {
          // Pop back to parent object
          header = (ObjectNode) nodeStack.pop();
        }
        break;

      case VALUE_STRING:
        header.put(currentFieldName, parser.getText());
        break;

      case VALUE_NUMBER_INT:
        header.put(currentFieldName, parser.getLongValue());
        break;

      case VALUE_NUMBER_FLOAT:
        header.put(currentFieldName, parser.getDecimalValue());
        break;

      case VALUE_TRUE:
        header.put(currentFieldName, true);
        break;

      case VALUE_FALSE:
        header.put(currentFieldName, false);
        break;

      case VALUE_NULL:
        header.putNull(currentFieldName);
        break;

      default:
        break;
    }
  }

  /**
   * Handles tokens inside epcisBody looking for eventList.
   */
  private void handleInEpcisBody(JsonToken token) throws IOException {
    switch (token) {
      case FIELD_NAME:
        currentFieldName = parser.currentName();
        if (EVENT_LIST_IN_CAMEL_CASE.equals(currentFieldName)) {
          // Found eventList - next token should be START_ARRAY
        }
        break;

      case START_ARRAY:
        if (EVENT_LIST_IN_CAMEL_CASE.equals(currentFieldName)) {
          // Detect early-eventList: eventList appears before @context (only on first pass)
          if (!retryPass && !header.has(CONTEXT)) {
            earlyEventListDetected = true;
          }
          state = State.IN_EVENT_LIST;
          eventListArrayDepth = 1;
        } else {
          // Skip other arrays in epcisBody
          skipCurrentStructure();
        }
        break;

      case START_OBJECT:
        epcisBodyDepth++;
        break;

      case END_OBJECT:
        epcisBodyDepth--;
        if (epcisBodyDepth <= 0) {
          // epcisBody closed, back to header level
          state = State.READING_HEADER;
        }
        break;

      default:
        // Skip scalar values
        break;
    }
  }

  /**
   * Handles tokens inside eventList array.
   */
  private void handleInEventList(JsonToken token) throws IOException {
    switch (token) {
      case START_OBJECT:
        if (earlyEventListDetected) {
          // Skip events on first pass when early-eventList detected; retry will handle them
          skipCurrentStructure();
        } else {
          // Start new event
          currentEvent = NODE_FACTORY.objectNode();
          eventDepth = 1;
          state = State.PARSING_EVENT;
        }
        break;

      case END_ARRAY:
        eventListArrayDepth--;
        if (eventListArrayDepth == 0) {
          // eventList closed
          state = State.IN_EPCIS_BODY;

          // If we haven't emitted header yet and it's valid, emit it now
          // On retry pass, skip header (already emitted on first pass)
          if (!headerEmitted && !retryPass && ObjectNodeUtil.isValidEPCISDocumentNode(header)) {
            emitHeader();
          }
        }
        break;

      case START_ARRAY:
        // Nested array - shouldn't happen in well-formed EPCIS but handle gracefully
        eventListArrayDepth++;
        break;

      default:
        // Ignore other tokens at eventList level
        break;
    }
  }

  /**
   * Handles tokens while parsing an individual event.
   */
  private void handleParsingEvent(JsonToken token) throws IOException {
    switch (token) {
      case FIELD_NAME:
        currentFieldName = parser.currentName();
        break;

      case START_OBJECT:
        eventDepth++;
        ObjectNode nested = NODE_FACTORY.objectNode();
        if (nodeStack.isEmpty()) {
          currentEvent.set(currentFieldName, nested);
        } else {
          ContainerNode<?> parent = nodeStack.peek();
          // Modern Java pattern matching for instanceof
          if (parent instanceof ObjectNode obj) {
            obj.set(currentFieldName, nested);
          } else if (parent instanceof ArrayNode arrNode) {
            arrNode.add(nested);
          }
        }
        nodeStack.push(nested);
        break;

      case START_ARRAY:
        eventDepth++;
        ArrayNode arr = NODE_FACTORY.arrayNode();
        if (nodeStack.isEmpty()) {
          currentEvent.set(currentFieldName, arr);
        } else {
          ContainerNode<?> parent = nodeStack.peek();
          // Modern Java pattern matching for instanceof
          if (parent instanceof ObjectNode obj) {
            obj.set(currentFieldName, arr);
          } else if (parent instanceof ArrayNode arrNode) {
            arrNode.add(arr);
          }
        }
        nodeStack.push(arr);
        break;

      case END_OBJECT:
        eventDepth--;
        if (eventDepth == 0) {
          // Event complete
          if (currentEvent.has(TYPE)) {
            // Emit header first if not done (skip on retry - already emitted on first pass)
            if (!headerEmitted && !retryPass && ObjectNodeUtil.isValidEPCISDocumentNode(header)) {
              emitHeader();
            }
            readyNodes.add(currentEvent);
          }
          currentEvent = null;
          nodeStack.clear();
          state = State.IN_EVENT_LIST;
        } else if (!nodeStack.isEmpty()) {
          // Only pop if stack has elements (defensive against malformed JSON)
          nodeStack.pop();
        }
        break;

      case END_ARRAY:
        eventDepth--;
        if (!nodeStack.isEmpty()) {
          // Only pop if stack has elements (defensive against malformed JSON)
          nodeStack.pop();
        }
        break;

      case VALUE_STRING:
        addValueToCurrentContainer(NODE_FACTORY.textNode(parser.getText()));
        break;

      case VALUE_NUMBER_INT:
        addValueToCurrentContainer(NODE_FACTORY.numberNode(parser.getLongValue()));
        break;

      case VALUE_NUMBER_FLOAT:
        addValueToCurrentContainer(NODE_FACTORY.numberNode(parser.getDecimalValue()));
        break;

      case VALUE_TRUE:
        addValueToCurrentContainer(NODE_FACTORY.booleanNode(true));
        break;

      case VALUE_FALSE:
        addValueToCurrentContainer(NODE_FACTORY.booleanNode(false));
        break;

      case VALUE_NULL:
        addValueToCurrentContainer(NODE_FACTORY.nullNode());
        break;

      default:
        break;
    }
  }

  /**
   * Adds a value node to the current container (object or array).
   */
  private void addValueToCurrentContainer(JsonNode value) {
    if (nodeStack.isEmpty()) {
      currentEvent.set(currentFieldName, value);
    } else {
      ContainerNode<?> parent = nodeStack.peek();
      // Modern Java pattern matching for instanceof
      if (parent instanceof ObjectNode obj) {
        obj.set(currentFieldName, value);
      } else if (parent instanceof ArrayNode arr) {
        arr.add(value);
      }
    }
  }

  /**
   * Reads an array into the specified parent and returns when complete.
   * Used for header arrays like @context.
   *
   * @param arr the array node to populate
   * @param currentDepth current nesting depth for overflow protection
   * @throws IOException if parsing error or nesting depth exceeded
   */
  private void readArrayIntoParent(ArrayNode arr, int currentDepth) throws IOException {
    if (currentDepth > MAX_NESTING_DEPTH) {
      throw new IOException("JSON nesting depth exceeds maximum of " + MAX_NESTING_DEPTH);
    }

    int depth = 1;
    JsonToken token;

    while (depth > 0 && (token = parser.nextToken()) != JsonToken.NOT_AVAILABLE) {
      switch (token) {
        case START_ARRAY:
          depth++;
          ArrayNode nested = NODE_FACTORY.arrayNode();
          arr.add(nested);
          // Recurse with incremented depth
          readArrayIntoParent(nested, currentDepth + 1);
          depth--;
          break;

        case END_ARRAY:
          depth--;
          break;

        case START_OBJECT:
          ObjectNode obj = NODE_FACTORY.objectNode();
          arr.add(obj);
          readObjectIntoParent(obj, currentDepth + 1);
          break;

        case VALUE_STRING:
          arr.add(parser.getText());
          break;

        case VALUE_NUMBER_INT:
          arr.add(parser.getLongValue());
          break;

        case VALUE_NUMBER_FLOAT:
          arr.add(parser.getDecimalValue());
          break;

        case VALUE_TRUE:
          arr.add(true);
          break;

        case VALUE_FALSE:
          arr.add(false);
          break;

        case VALUE_NULL:
          arr.addNull();
          break;

        default:
          break;
      }
    }

    // Pop back to header with defensive type check
    if (!nodeStack.isEmpty()) {
      ContainerNode<?> popped = nodeStack.pop();
      if (popped instanceof ObjectNode obj) {
        header = obj;
      }
    }
  }

  /**
   * Reads an object into the specified parent.
   *
   * @param obj the object node to populate
   * @param currentDepth current nesting depth for overflow protection
   * @throws IOException if parsing error or nesting depth exceeded
   */
  private void readObjectIntoParent(ObjectNode obj, int currentDepth) throws IOException {
    if (currentDepth > MAX_NESTING_DEPTH) {
      throw new IOException("JSON nesting depth exceeds maximum of " + MAX_NESTING_DEPTH);
    }

    int depth = 1;
    JsonToken token;
    String fieldName = null;

    while (depth > 0 && (token = parser.nextToken()) != JsonToken.NOT_AVAILABLE) {
      switch (token) {
        case FIELD_NAME:
          fieldName = parser.currentName();
          break;

        case START_OBJECT:
          depth++;
          ObjectNode nested = NODE_FACTORY.objectNode();
          obj.set(fieldName, nested);
          readObjectIntoParent(nested, currentDepth + 1);
          depth--;
          break;

        case END_OBJECT:
          depth--;
          break;

        case START_ARRAY:
          ArrayNode arr = NODE_FACTORY.arrayNode();
          obj.set(fieldName, arr);
          readArrayIntoParent(arr, currentDepth + 1);
          break;

        case VALUE_STRING:
          obj.put(fieldName, parser.getText());
          break;

        case VALUE_NUMBER_INT:
          obj.put(fieldName, parser.getLongValue());
          break;

        case VALUE_NUMBER_FLOAT:
          obj.put(fieldName, parser.getDecimalValue());
          break;

        case VALUE_TRUE:
          obj.put(fieldName, true);
          break;

        case VALUE_FALSE:
          obj.put(fieldName, false);
          break;

        case VALUE_NULL:
          obj.putNull(fieldName);
          break;

        default:
          break;
      }
    }
  }

  /**
   * Skips the current structure (object or array).
   */
  private void skipCurrentStructure() throws IOException {
    int depth = 1;
    JsonToken token;

    while (depth > 0 && (token = parser.nextToken()) != JsonToken.NOT_AVAILABLE) {
      if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
        depth++;
      } else if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
        depth--;
      }
    }
  }

  /**
   * Emits the header node with epcisBody placeholder at the end.
   */
  private void emitHeader() {
    // Move epcisBody to the end if present
    if (header.has(EPCIS_BODY_IN_CAMEL_CASE)) {
      header.remove(EPCIS_BODY_IN_CAMEL_CASE);
    }
    header.set(EPCIS_BODY_IN_CAMEL_CASE, NODE_FACTORY.nullNode());

    readyNodes.add(header);
    headerEmitted = true;
  }

  /**
   * Clears internal parser state to free memory.
   *
   * <p>Call this after an error to ensure no stale references are held.
   * This is automatically called by {@link #close()}.
   */
  public void clearState() {
    nodeStack.clear();
    currentEvent = null;
    currentFieldName = null;
    readyNodes.clear();
  }

  /**
   * Closes the parser and releases resources.
   *
   * <p>Clears all internal state and closes the underlying JSON parser.
   */
  public void close() {
    clearState();
    try {
      parser.close();
    } catch (IOException e) {
      log.log(System.Logger.Level.WARNING, "Error closing parser", e);
    }
  }
}
