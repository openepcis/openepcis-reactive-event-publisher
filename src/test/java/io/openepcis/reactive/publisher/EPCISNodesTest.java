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
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for EPCISNodes utility class.
 */
public class EPCISNodesTest {

  private static List<ObjectNode> threeEventsNodes;

  @BeforeAll
  static void setup() throws IOException {
    byte[] json = readResource("/object-node-publisher/ThreeEvents.json");
    threeEventsNodes = ObjectNodePublisher.parseAll(json);
  }

  @Test
  public void testIsHeader() {
    ObjectNode header = EPCISNodes.header(threeEventsNodes).orElseThrow();
    assertTrue(EPCISNodes.isHeader(header));

    ObjectNode event = EPCISNodes.events(threeEventsNodes).get(0);
    assertFalse(EPCISNodes.isHeader(event));

    assertFalse(EPCISNodes.isHeader(null));
  }

  @Test
  public void testIsEvent() {
    ObjectNode header = EPCISNodes.header(threeEventsNodes).orElseThrow();
    assertFalse(EPCISNodes.isEvent(header));

    ObjectNode event = EPCISNodes.events(threeEventsNodes).get(0);
    assertTrue(EPCISNodes.isEvent(event));

    assertFalse(EPCISNodes.isEvent(null));
  }

  @Test
  public void testIsEventType() {
    List<ObjectNode> events = EPCISNodes.events(threeEventsNodes);
    assertTrue(events.stream().anyMatch(e -> EPCISNodes.isEventType(e, "ObjectEvent")));
    assertFalse(events.stream().anyMatch(e -> EPCISNodes.isEventType(e, "NonExistentEvent")));
  }

  @Test
  public void testHeader() {
    assertTrue(EPCISNodes.header(threeEventsNodes).isPresent());
    assertEquals(EPCIS.EPCIS_DOCUMENT, EPCISNodes.header(threeEventsNodes).get().path(EPCIS.TYPE).asText());

    assertTrue(EPCISNodes.header(null).isEmpty());
    assertTrue(EPCISNodes.header(List.of()).isEmpty());
  }

  @Test
  public void testEvents() {
    List<ObjectNode> events = EPCISNodes.events(threeEventsNodes);
    assertEquals(3, events.size());
    assertTrue(events.stream().allMatch(EPCISNodes::isEvent));

    assertTrue(EPCISNodes.events(null).isEmpty());
    assertTrue(EPCISNodes.events(List.of()).isEmpty());
  }

  @Test
  public void testFilterByType() {
    List<ObjectNode> objectEvents = EPCISNodes.filterByType(threeEventsNodes, "ObjectEvent");
    assertFalse(objectEvents.isEmpty());
    assertTrue(objectEvents.stream().allMatch(e -> "ObjectEvent".equals(e.path(EPCIS.TYPE).asText())));

    List<ObjectNode> nonExistent = EPCISNodes.filterByType(threeEventsNodes, "NonExistent");
    assertTrue(nonExistent.isEmpty());

    assertTrue(EPCISNodes.filterByType(null, "ObjectEvent").isEmpty());
    assertTrue(EPCISNodes.filterByType(threeEventsNodes, null).isEmpty());
  }

  @Test
  public void testFilter() {
    // Filter events with specific bizStep
    List<ObjectNode> filtered = EPCISNodes.filter(threeEventsNodes,
        n -> n.has("bizStep"));
    assertFalse(filtered.isEmpty());

    assertTrue(EPCISNodes.filter(null, n -> true).isEmpty());
    assertTrue(EPCISNodes.filter(threeEventsNodes, null).isEmpty());
  }

  @Test
  public void testGetType() {
    ObjectNode header = EPCISNodes.header(threeEventsNodes).orElseThrow();
    assertEquals(EPCIS.EPCIS_DOCUMENT, EPCISNodes.getType(header));

    assertEquals("", EPCISNodes.getType(null));
  }

  @Test
  public void testGetEventTime() {
    ObjectNode event = EPCISNodes.events(threeEventsNodes).get(0);
    assertFalse(EPCISNodes.getEventTime(event).isEmpty());

    assertEquals("", EPCISNodes.getEventTime(null));
  }

  @Test
  public void testCountEvents() {
    assertEquals(3, EPCISNodes.countEvents(threeEventsNodes));
    assertEquals(0, EPCISNodes.countEvents(null));
    assertEquals(0, EPCISNodes.countEvents(List.of()));
  }

  @Test
  public void testHasEvents() {
    assertTrue(EPCISNodes.hasEvents(threeEventsNodes));
    assertFalse(EPCISNodes.hasEvents(null));
    assertFalse(EPCISNodes.hasEvents(List.of()));
  }

  @Test
  public void testHasHeader() {
    assertTrue(EPCISNodes.hasHeader(threeEventsNodes));
    assertFalse(EPCISNodes.hasHeader(null));
    assertFalse(EPCISNodes.hasHeader(List.of()));
  }

  // Helper
  private static byte[] readResource(String path) throws IOException {
    try (InputStream is = EPCISNodesTest.class.getResourceAsStream(path)) {
      if (is == null) {
        throw new IOException("Resource not found: " + path);
      }
      return is.readAllBytes();
    }
  }
}
