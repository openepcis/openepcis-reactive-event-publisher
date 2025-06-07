/*
 * Copyright 2022-2024 benelog GmbH & Co. KG
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
import io.openepcis.constants.EPCIS;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class ObjectNodePublisherTest {

  private static final Map<String, Integer> SIMPLE_TESTS =
      new HashMap<>() {
        {
          put("/object-node-publisher/OneEvent.json", 2);
          put("/object-node-publisher/TwoEvents.json", 3);
          put("/object-node-publisher/ThreeEvents.json", 4);
        }
      };
  private static final Map<String, Integer> EARLY_EVENT_LIST_TESTS =
      new HashMap<>() {
        {
          put("/object-node-publisher/OneEvent-earlyEventList.json", 2);
          put("/object-node-publisher/TwoEvents-earlyEventList.json", 3);
          put("/object-node-publisher/ThreeEvents-earlyEventList.json", 4);

          put("/object-node-publisher/OneEvent-earlyEventList-contextAtEnd.json", 2);
          put("/object-node-publisher/TwoEvents-earlyEventList-contextAtEnd.json", 3);
          put("/object-node-publisher/ThreeEvents-earlyEventList-contextAtEnd.json", 4);
        }
      };

  @Test
  public void testQueryDocument() throws IOException {
    final ObjectNodePublisher<ObjectNode> publisher =
        new ObjectNodePublisher<>(
            getClass().getResourceAsStream("/object-node-publisher/QueryDocument.json"));
    final var result = Multi.createFrom().publisher(publisher).subscribe().asStream().toList();
    assertNotNull(result);
    assertEquals(5, result.size());
  }

  @Test
  public void testEmpty() throws IOException {
    final StringReader reader = new StringReader("{}");
    final ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(reader);
    final var result = Multi.createFrom().publisher(publisher).subscribe().asStream().toList();
    assertNotNull(result);
    assertEquals(0, result.size());
  }

  @Test
  public void testEmptyEventList() throws IOException {
    final ObjectNodePublisher<ObjectNode> publisher =
        new ObjectNodePublisher<>(
            getClass().getResourceAsStream("/object-node-publisher/EmptyEventList.json"));
    final var result = Multi.createFrom().publisher(publisher).subscribe().asStream().toList();
    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, result.get(0).get(EPCIS.TYPE).asText());
  }

  @Test
  public void testSimple() throws IOException {
    for (Map.Entry<String, Integer> e : SIMPLE_TESTS.entrySet()) {
      validateSimple(e.getKey(), e.getValue());
    }
  }

  private void validateSimple(final String jsonUrl, final int expected) throws IOException {
    final ObjectNodePublisher<ObjectNode> publisher =
        new ObjectNodePublisher<>(getClass().getResourceAsStream(jsonUrl));
    final var result = Multi.createFrom().publisher(publisher).subscribe().asStream().toList();
    assertNotNull(result);
    assertEquals(expected, result.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, result.get(0).get(EPCIS.TYPE).asText());
  }

  @Test
  public void testEarlyEventList() throws IOException {
    for (Map.Entry<String, Integer> e : EARLY_EVENT_LIST_TESTS.entrySet()) {
      validateEarlyEventList(e.getKey(), e.getValue());
    }
  }

  private void validateEarlyEventList(final String jsonUrl, final int expected) throws IOException {
    final ObjectNodePublisher<ObjectNode> publisher =
        new ObjectNodePublisher<>(getClass().getResourceAsStream(jsonUrl), () -> getClass().getResourceAsStream(jsonUrl));
    final var result = Multi.createFrom().publisher(publisher).subscribe().asStream().toList();
    assertNotNull(result);
    assertEquals(expected, result.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, result.get(0).get(EPCIS.TYPE).asText());
  }

  @Test
  public void testInvalidInput() throws IOException {
    final ObjectNodePublisher<ObjectNode> publisher =
        new ObjectNodePublisher<>(
            getClass().getResourceAsStream("/object-node-publisher/PrematureEOF.json"));
    assertThrows(
        Exception.class,
        () -> Multi.createFrom().publisher(publisher).subscribe().asStream().toList());
  }

  @Test
  public void testIgnoreEventList() throws Exception {
    final String jsonUrl = "/object-node-publisher/ThreeEvents-earlyEventList-contextAtEnd.json";

    // ignore all eventList nodes
    final ObjectNodePublisher<ObjectNode> publisher =
        new ObjectNodePublisher<>(getClass().getResourceAsStream(jsonUrl));
    publisher.setIgnoreEventList(true);
    var result = Multi.createFrom().publisher(publisher).subscribe().asStream().toList();
    assertEquals(1, result.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, result.get(0).get(EPCIS.TYPE).asText());

    // ignore after 1 eventList item
    final List<ObjectNode> resultsIgnoreAfterFirst = new ArrayList<>();
    final ObjectNodePublisher<ObjectNode> publisherIgnoreAfterFirst =
        new ObjectNodePublisher<>(getClass().getResourceAsStream(jsonUrl));
    final AtomicBoolean running = new AtomicBoolean(true);
    Multi.createFrom()
        .publisher(publisherIgnoreAfterFirst)
        .subscribe()
        .with(
            jsonNodes -> {
              resultsIgnoreAfterFirst.add(jsonNodes);
              if (resultsIgnoreAfterFirst.size() == 1) {
                publisherIgnoreAfterFirst.setIgnoreEventList(true);
              }
            },
            () -> running.getAndSet(false));
    while (running.get()) {
      Thread.yield();
    }
    assertEquals(1, resultsIgnoreAfterFirst.size());
    assertEquals(
        EPCIS.EPCIS_DOCUMENT,
        resultsIgnoreAfterFirst.get(resultsIgnoreAfterFirst.size() - 1).get(EPCIS.TYPE).asText());
  }
}
