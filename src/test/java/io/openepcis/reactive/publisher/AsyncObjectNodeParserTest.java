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

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openepcis.constants.EPCIS;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for AsyncObjectNodeParser.
 */
public class AsyncObjectNodeParserTest {

  @Test
  public void testParseSimpleDocument() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/OneEvent.json");

    // Feed all at once
    AsyncObjectNodeParser.FeedResult result = parser.feedInput(json, 0, json.length);
    parser.endOfInput();

    // Should have header + 1 event = 2 nodes
    List<ObjectNode> nodes = drainNodes(parser);

    assertEquals(2, nodes.size());

    // First should be header
    ObjectNode header = nodes.get(0);
    assertEquals(EPCIS.EPCIS_DOCUMENT, header.get(EPCIS.TYPE).asText());
    assertTrue(header.has("@context"));
    assertTrue(header.has("schemaVersion"));

    // Second should be event
    ObjectNode event = nodes.get(1);
    assertEquals("TransformationEvent", event.get(EPCIS.TYPE).asText());
    assertTrue(event.has("eventTime"));
  }

  @Test
  public void testParseMultipleEvents() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/ThreeEvents.json");
    parser.feedInput(json, 0, json.length);
    parser.endOfInput();

    List<ObjectNode> nodes = drainNodes(parser);

    // Header + 3 events = 4 nodes
    assertEquals(4, nodes.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(0).get(EPCIS.TYPE).asText());
  }

  @Test
  public void testParseChunkedInput() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/TwoEvents.json");

    // Feed in small chunks (simulating network)
    int chunkSize = 64;
    for (int i = 0; i < json.length; i += chunkSize) {
      int len = Math.min(chunkSize, json.length - i);
      parser.feedInput(json, i, len);
    }
    parser.endOfInput();

    List<ObjectNode> nodes = drainNodes(parser);

    // Header + 2 events = 3 nodes
    assertEquals(3, nodes.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(0).get(EPCIS.TYPE).asText());
  }

  @Test
  public void testParseByteBuffer() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/OneEvent.json");
    ByteBuffer buffer = ByteBuffer.wrap(json);

    parser.feedInput(buffer);
    parser.endOfInput();

    List<ObjectNode> nodes = drainNodes(parser);
    assertEquals(2, nodes.size());
  }

  @Test
  public void testParseDirectByteBuffer() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/OneEvent.json");
    ByteBuffer buffer = ByteBuffer.allocateDirect(json.length);
    buffer.put(json);
    buffer.flip();

    parser.feedInput(buffer);
    parser.endOfInput();

    List<ObjectNode> nodes = drainNodes(parser);
    assertEquals(2, nodes.size());
  }

  @Test
  public void testEmptyEventList() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/EmptyEventList.json");
    parser.feedInput(json, 0, json.length);
    parser.endOfInput();

    List<ObjectNode> nodes = drainNodes(parser);

    // Should have just the header
    assertEquals(1, nodes.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(0).get(EPCIS.TYPE).asText());
  }

  @Test
  public void testQueryDocument() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/QueryDocument.json");
    parser.feedInput(json, 0, json.length);
    parser.endOfInput();

    List<ObjectNode> nodes = drainNodes(parser);

    // Should parse query response with events
    assertFalse(nodes.isEmpty());
  }

  // ========== Early-eventList Detection Tests ==========

  @Test
  public void testEarlyEventListNotDetectedForNormalDocument() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/ThreeEvents.json");
    parser.feedInput(json, 0, json.length);
    parser.endOfInput();

    // Normal document should not trigger early-eventList detection
    assertFalse(parser.isEarlyEventListDetected());

    List<ObjectNode> nodes = drainNodes(parser);

    // Should have header + 3 events
    assertEquals(4, nodes.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(0).get(EPCIS.TYPE).asText());
  }

  @Test
  public void testEarlyEventListDetectedWhenEventListBeforeContext() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    // Document where eventList appears before @context
    byte[] json = readResource("/object-node-publisher/TwoEvents-earlyEventList.json");
    parser.feedInput(json, 0, json.length);
    parser.endOfInput();

    // Should detect early-eventList
    assertTrue(parser.isEarlyEventListDetected());

    List<ObjectNode> nodes = drainNodes(parser);

    // On first pass with earlyEventListDetected, only header is emitted (events skipped)
    assertEquals(1, nodes.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(0).get(EPCIS.TYPE).asText());
  }

  @Test
  public void testEarlyEventListWithContextAtEnd() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    // Document where @context appears at the very end
    byte[] json = readResource("/object-node-publisher/TwoEvents-earlyEventList-contextAtEnd.json");
    parser.feedInput(json, 0, json.length);
    parser.endOfInput();

    // Should detect early-eventList
    assertTrue(parser.isEarlyEventListDetected());

    List<ObjectNode> nodes = drainNodes(parser);

    // Header should still be emitted with @context (collected at end)
    assertEquals(1, nodes.size());
    ObjectNode header = nodes.get(0);
    assertEquals(EPCIS.EPCIS_DOCUMENT, header.get(EPCIS.TYPE).asText());
    assertTrue(header.has(EPCIS.CONTEXT), "Header should have @context even if it appeared at end");
  }

  @Test
  public void testRetryPassProcessesEventsNormally() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();
    parser.setRetryPass();  // Mark as retry

    // Same early-eventList document
    byte[] json = readResource("/object-node-publisher/TwoEvents-earlyEventList.json");
    parser.feedInput(json, 0, json.length);
    parser.endOfInput();

    // On retry pass, earlyEventListDetected should NOT be set
    assertFalse(parser.isEarlyEventListDetected());

    List<ObjectNode> nodes = drainNodes(parser);

    // On retry pass, only events are emitted (header skipped since it was emitted on first pass)
    assertEquals(2, nodes.size());
    // Both should be events (no header)
    for (ObjectNode node : nodes) {
      assertNotEquals(EPCIS.EPCIS_DOCUMENT, node.get(EPCIS.TYPE).asText());
    }
  }

  @Test
  public void testRetryPassSkipsHeaderEmission() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();
    parser.setRetryPass();  // Mark as retry

    // Normal document (not early-eventList)
    byte[] json = readResource("/object-node-publisher/TwoEvents.json");
    parser.feedInput(json, 0, json.length);
    parser.endOfInput();

    List<ObjectNode> nodes = drainNodes(parser);

    // On retry pass, header is skipped, only events emitted
    assertEquals(2, nodes.size());
    for (ObjectNode node : nodes) {
      assertNotEquals(EPCIS.EPCIS_DOCUMENT, node.get(EPCIS.TYPE).asText());
    }
  }

  @Test
  public void testEarlyEventListWithMultipleEvents() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/ThreeEvents-earlyEventList.json");
    parser.feedInput(json, 0, json.length);
    parser.endOfInput();

    assertTrue(parser.isEarlyEventListDetected());

    List<ObjectNode> nodes = drainNodes(parser);

    // First pass: only header (3 events skipped)
    assertEquals(1, nodes.size());
  }

  @Test
  public void testNeedsMoreInput() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    // Feed partial JSON
    String partial = "{\"@context\":[";
    byte[] bytes = partial.getBytes(StandardCharsets.UTF_8);

    AsyncObjectNodeParser.FeedResult result = parser.feedInput(bytes, 0, bytes.length);

    // Should need more input
    assertEquals(AsyncObjectNodeParser.FeedResult.NEED_MORE_INPUT, result);
    assertTrue(parser.needsMoreInput());
    assertFalse(parser.isComplete());
  }

  @Test
  public void testIncrementalNodeAvailability() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/ThreeEvents.json");

    // Feed in larger chunks - should get nodes incrementally
    int chunkSize = 512;
    List<ObjectNode> allNodes = new ArrayList<>();

    for (int i = 0; i < json.length; i += chunkSize) {
      int len = Math.min(chunkSize, json.length - i);
      AsyncObjectNodeParser.FeedResult result = parser.feedInput(json, i, len);

      // Drain any available nodes
      ObjectNode node;
      while ((node = parser.pollNextNode()) != null) {
        allNodes.add(node);
      }
    }

    parser.endOfInput();

    // Get any remaining nodes
    ObjectNode node;
    while ((node = parser.pollNextNode()) != null) {
      allNodes.add(node);
    }

    assertEquals(4, allNodes.size()); // header + 3 events
  }

  @Test
  public void testParserClose() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/OneEvent.json");
    parser.feedInput(json, 0, json.length);

    // Should not throw
    parser.close();
  }

  @Test
  public void testClearState() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/OneEvent.json");
    parser.feedInput(json, 0, json.length);
    parser.endOfInput();

    // Verify nodes are available
    assertTrue(parser.hasMoreNodes());

    // Clear state
    parser.clearState();

    // Verify state is cleared
    assertFalse(parser.hasMoreNodes());
    assertNull(parser.pollNextNode());
  }

  @Test
  public void testDeeplyNestedJsonInHeader() throws IOException {
    // Create JSON with deep nesting in @context array (exceeding MAX_NESTING_DEPTH=1000)
    // This would trigger StackOverflowError without depth limit
    StringBuilder sb = new StringBuilder();
    sb.append("{\"@context\":[");
    for (int i = 0; i < 1100; i++) {
      sb.append("[");
    }
    sb.append("\"value\"");
    for (int i = 0; i < 1100; i++) {
      sb.append("]");
    }
    sb.append("],\"type\":\"EPCISDocument\"}");

    byte[] json = sb.toString().getBytes(StandardCharsets.UTF_8);

    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    // Should throw IOException due to depth limit
    assertThrows(IOException.class, () -> {
      parser.feedInput(json, 0, json.length);
    });
  }

  @Test
  public void testMalformedJsonThrowsException() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    // Incomplete JSON - missing closing braces
    String malformed = "{\"@context\":[\"https://example.com\"],\"type\":\"EPCISDocument\",\"epcisBody\":{\"eventList\":[{\"type\":\"ObjectEvent\"";
    byte[] json = malformed.getBytes(StandardCharsets.UTF_8);

    // Feed the incomplete JSON
    parser.feedInput(json, 0, json.length);

    // End of input with incomplete structure - Jackson properly throws
    assertThrows(IOException.class, () -> parser.endOfInput());
  }

  @Test
  public void testExtraClosingBracesThrowsException() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    // JSON with extra closing braces (malformed)
    String malformed = "{\"@context\":[],\"type\":\"EPCISDocument\"}}}}";
    byte[] json = malformed.getBytes(StandardCharsets.UTF_8);

    // Jackson properly throws on malformed JSON with mismatched brackets
    assertThrows(IOException.class, () -> parser.feedInput(json, 0, json.length));
  }

  // Helper methods

  private List<ObjectNode> drainNodes(AsyncObjectNodeParser parser) {
    List<ObjectNode> nodes = new ArrayList<>();
    ObjectNode node;
    while ((node = parser.pollNextNode()) != null) {
      nodes.add(node);
    }
    return nodes;
  }

  private byte[] readResource(String path) throws IOException {
    try (InputStream is = getClass().getResourceAsStream(path)) {
      if (is == null) {
        throw new IOException("Resource not found: " + path);
      }
      return is.readAllBytes();
    }
  }
}
