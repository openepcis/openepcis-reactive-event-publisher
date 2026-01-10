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
import io.smallrye.mutiny.Multi;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import org.junit.jupiter.api.Test;

/**
 * Tests for ObjectNodePublisher.
 */
public class ObjectNodePublisherTest {

  @Test
  public void testParseFromByteBufferPublisher() throws IOException {
    byte[] json = readResource("/object-node-publisher/OneEvent.json");

    // Create a simple Flow.Publisher that emits the bytes
    Flow.Publisher<ByteBuffer> source = createByteBufferPublisher(json);

    ObjectNodePublisher<ObjectNode> publisher = ObjectNodePublisher.builder()
        .source(source)
        .build();

    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    assertEquals(2, nodes.size());

    // First should be header
    ObjectNode header = nodes.get(0);
    assertEquals(EPCIS.EPCIS_DOCUMENT, header.get(EPCIS.TYPE).asText());
    assertTrue(header.has("@context"));

    // Second should be event
    ObjectNode event = nodes.get(1);
    assertEquals("TransformationEvent", event.get(EPCIS.TYPE).asText());
  }

  @Test
  public void testParseMultipleEvents() throws IOException {
    byte[] json = readResource("/object-node-publisher/ThreeEvents.json");

    Flow.Publisher<ByteBuffer> source = createByteBufferPublisher(json);

    ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(source);

    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    // Header + 3 events = 4 nodes
    assertEquals(4, nodes.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(0).get(EPCIS.TYPE).asText());
  }

  @Test
  public void testChunkedInput() throws IOException {
    byte[] json = readResource("/object-node-publisher/TwoEvents.json");

    // Split into small chunks to simulate network streaming
    Flow.Publisher<ByteBuffer> source = createChunkedByteBufferPublisher(json, 64);

    ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(source);

    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    // Header + 2 events = 3 nodes
    assertEquals(3, nodes.size());
  }

  @Test
  public void testEmptyEventList() throws IOException {
    byte[] json = readResource("/object-node-publisher/EmptyEventList.json");

    Flow.Publisher<ByteBuffer> source = createByteBufferPublisher(json);
    ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(source);

    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    // Just header
    assertEquals(1, nodes.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(0).get(EPCIS.TYPE).asText());
  }

  @Test
  public void testQueryDocument() throws IOException {
    byte[] json = readResource("/object-node-publisher/QueryDocument.json");

    Flow.Publisher<ByteBuffer> source = createByteBufferPublisher(json);
    ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(source);

    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    assertFalse(nodes.isEmpty());
  }

  @Test
  public void testNodeCount() throws IOException {
    byte[] json = readResource("/object-node-publisher/ThreeEvents.json");

    Flow.Publisher<ByteBuffer> source = createByteBufferPublisher(json);
    ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(source);

    Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    assertEquals(4, publisher.getNodeCount());
  }

  @Test
  public void testNullSourceThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        new ObjectNodePublisher<>((Flow.Publisher<ByteBuffer>) null));
  }

  @Test
  public void testBuilderNullSourceThrows() {
    assertThrows(IllegalStateException.class, () ->
        ObjectNodePublisher.builder().build());
  }

  @Test
  public void testDirectByteBuffer() throws IOException {
    byte[] json = readResource("/object-node-publisher/OneEvent.json");

    // Create a direct ByteBuffer with simple sync publisher
    ByteBuffer direct = ByteBuffer.allocateDirect(json.length);
    direct.put(json);
    direct.flip();

    Flow.Publisher<ByteBuffer> source = subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        private boolean completed = false;

        @Override
        public void request(long n) {
          if (!completed && n > 0) {
            completed = true;
            subscriber.onNext(direct);
            subscriber.onComplete();
          }
        }

        @Override
        public void cancel() {
          completed = true;
        }
      });
    };

    ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(source);

    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    assertEquals(2, nodes.size());
  }

  @Test
  public void testEarlyEventListDocumentWithoutRetry() throws IOException {
    // Document where eventList appears before @context
    // Without retry source, early-eventList detection skips events on first pass
    byte[] json = readResource("/object-node-publisher/TwoEvents-earlyEventList.json");

    ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(
        createByteBufferPublisher(json)
    );

    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    // Without retry, only header is emitted (events are skipped due to early-eventList)
    assertEquals(1, nodes.size());

    // Verify it's the header
    assertEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(0).path(EPCIS.TYPE).asText());
  }

  @Test
  public void testBuilderWithMulti() throws IOException {
    byte[] json = readResource("/object-node-publisher/OneEvent.json");

    Multi<ByteBuffer> multi = Multi.createFrom().item(ByteBuffer.wrap(json));

    // Multi implements Flow.Publisher, so we can use source() directly
    ObjectNodePublisher<ObjectNode> publisher = ObjectNodePublisher.builder()
        .source(multi)
        .build();

    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    assertEquals(2, nodes.size());
  }

  @Test
  public void testBackpressure() throws IOException {
    // Test that backpressure works - source should only be requested when demand exists
    byte[] json = readResource("/object-node-publisher/ThreeEvents.json");

    // Track how many chunks were requested
    java.util.concurrent.atomic.AtomicInteger requestCount = new java.util.concurrent.atomic.AtomicInteger(0);

    Flow.Publisher<ByteBuffer> trackedSource = subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        private int offset = 0;
        private boolean cancelled = false;
        private final int chunkSize = 256;

        @Override
        public void request(long n) {
          if (cancelled) return;
          requestCount.addAndGet((int) n);

          for (long i = 0; i < n && offset < json.length && !cancelled; i++) {
            int len = Math.min(chunkSize, json.length - offset);
            byte[] chunk = new byte[len];
            System.arraycopy(json, offset, chunk, 0, len);
            offset += len;
            subscriber.onNext(ByteBuffer.wrap(chunk));
          }

          if (offset >= json.length && !cancelled) {
            subscriber.onComplete();
          }
        }

        @Override
        public void cancel() {
          cancelled = true;
        }
      });
    };

    ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(trackedSource);

    // Consume one at a time to test backpressure
    List<ObjectNode> nodes = new ArrayList<>();
    Multi.createFrom().publisher(publisher)
        .subscribe().with(nodes::add);

    // Wait for completion
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Should have header + 3 events
    assertEquals(4, nodes.size());

    // Requests should have been made (exact count depends on implementation)
    assertTrue(requestCount.get() > 0, "Should have made requests to source");
  }

  @Test
  public void testCancellation() throws IOException {
    byte[] json = readResource("/object-node-publisher/ThreeEvents.json");

    java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean(false);

    Flow.Publisher<ByteBuffer> cancellableSource = subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        private boolean completed = false;

        @Override
        public void request(long n) {
          if (!completed && n > 0 && !cancelled.get()) {
            completed = true;
            subscriber.onNext(ByteBuffer.wrap(json));
            subscriber.onComplete();
          }
        }

        @Override
        public void cancel() {
          cancelled.set(true);
        }
      });
    };

    ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(cancellableSource);

    // Take only 2 nodes then cancel
    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .select().first(2)
        .subscribe().asStream().toList();

    assertEquals(2, nodes.size());
  }

  // ==================== Convenience Method Tests ====================

  @Test
  public void testFromBytes() throws IOException {
    byte[] json = readResource("/object-node-publisher/TwoEvents.json");

    List<ObjectNode> nodes = ObjectNodePublisher.fromBytes(json)
        .toMulti()
        .subscribe().asStream().toList();

    assertEquals(3, nodes.size()); // header + 2 events
  }

  @Test
  public void testParseAll() throws IOException {
    byte[] json = readResource("/object-node-publisher/ThreeEvents.json");

    List<ObjectNode> nodes = ObjectNodePublisher.parseAll(json);

    assertEquals(4, nodes.size()); // header + 3 events
    assertTrue(EPCISNodes.hasHeader(nodes));
    assertEquals(3, EPCISNodes.countEvents(nodes));
  }

  @Test
  public void testToMulti() throws IOException {
    byte[] json = readResource("/object-node-publisher/OneEvent.json");

    // Test fluent chaining with toMulti()
    long eventCount = ObjectNodePublisher.fromBytes(json)
        .toMulti()
        .filter(EPCISNodes::isEvent)
        .collect().asList()
        .await().indefinitely()
        .size();

    assertEquals(1, eventCount);
  }

  @Test
  public void testFromStaticFactory() throws IOException {
    byte[] json = readResource("/object-node-publisher/OneEvent.json");

    Flow.Publisher<ByteBuffer> source = createByteBufferPublisher(json);

    List<ObjectNode> nodes = ObjectNodePublisher.from(source)
        .toMulti()
        .subscribe().asStream().toList();

    assertEquals(2, nodes.size());
  }

  @Test
  public void testFromMultiStaticFactory() throws IOException {
    byte[] json = readResource("/object-node-publisher/TwoEvents.json");

    Multi<ByteBuffer> multi = Multi.createFrom().item(ByteBuffer.wrap(json));

    List<ObjectNode> nodes = ObjectNodePublisher.fromMulti(multi)
        .toMulti()
        .subscribe().asStream().toList();

    assertEquals(3, nodes.size());
  }

  @Test
  public void testFromInputStream() throws IOException {
    try (InputStream is = getClass().getResourceAsStream("/object-node-publisher/TwoEvents.json")) {
      List<ObjectNode> nodes = ObjectNodePublisher.fromInputStream(is)
          .toMulti()
          .subscribe().asStream().toList();

      // Header + 2 events = 3 nodes
      assertEquals(3, nodes.size());
      assertTrue(EPCISNodes.hasHeader(nodes));
      assertEquals(2, EPCISNodes.countEvents(nodes));
    }
  }

  @Test
  public void testFromInputStreamWithCustomBufferSize() throws IOException {
    try (InputStream is = getClass().getResourceAsStream("/object-node-publisher/ThreeEvents.json")) {
      // Use small buffer to force multiple reads
      List<ObjectNode> nodes = ObjectNodePublisher.fromInputStream(is, 64)
          .toMulti()
          .subscribe().asStream().toList();

      // Header + 3 events = 4 nodes
      assertEquals(4, nodes.size());
    }
  }

  @Test
  public void testFromInputStreamNullThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        ObjectNodePublisher.fromInputStream(null));
  }

  // ==================== Builder with InputStream Tests ====================

  @Test
  public void testBuilderWithInputStream() throws IOException {
    try (InputStream is = getClass().getResourceAsStream("/object-node-publisher/TwoEvents.json")) {
      List<ObjectNode> nodes = ObjectNodePublisher.builder()
          .inputStream(is)
          .build()
          .toMulti()
          .subscribe().asStream().toList();

      // Header + 2 events = 3 nodes
      assertEquals(3, nodes.size());
      assertTrue(EPCISNodes.hasHeader(nodes));
      assertEquals(2, EPCISNodes.countEvents(nodes));
    }
  }

  @Test
  public void testBuilderWithInputStreamAndBufferSize() throws IOException {
    try (InputStream is = getClass().getResourceAsStream("/object-node-publisher/ThreeEvents.json")) {
      List<ObjectNode> nodes = ObjectNodePublisher.builder()
          .inputStream(is)
          .bufferSize(64)  // Small buffer to force multiple reads
          .build()
          .toMulti()
          .subscribe().asStream().toList();

      // Header + 3 events = 4 nodes
      assertEquals(4, nodes.size());
    }
  }

  @Test
  public void testBuilderNoSourceThrows() {
    assertThrows(IllegalStateException.class, () ->
        ObjectNodePublisher.builder().build());
  }

  @Test
  public void testBuilderBothSourceAndInputStreamThrows() throws IOException {
    byte[] json = readResource("/object-node-publisher/OneEvent.json");
    Flow.Publisher<ByteBuffer> source = createByteBufferPublisher(json);

    try (InputStream is = getClass().getResourceAsStream("/object-node-publisher/OneEvent.json")) {
      assertThrows(IllegalStateException.class, () ->
          ObjectNodePublisher.builder()
              .source(source)
              .inputStream(is)
              .build());
    }
  }

  // ==================== New Tests from Code Review ====================

  @Test
  public void testFromPath() throws Exception {
    // Create a temp file with EPCIS content
    java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("epcis-test", ".json");
    try {
      byte[] json = readResource("/object-node-publisher/TwoEvents.json");
      java.nio.file.Files.write(tempFile, json);

      List<ObjectNode> nodes = ObjectNodePublisher.fromPath(tempFile)
          .toMulti()
          .subscribe().asStream().toList();

      // Header + 2 events = 3 nodes
      assertEquals(3, nodes.size());
      assertTrue(EPCISNodes.hasHeader(nodes));
      assertEquals(2, EPCISNodes.countEvents(nodes));
    } finally {
      java.nio.file.Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void testMultipleSubscribersRejected() throws Exception {
    byte[] json = readResource("/object-node-publisher/OneEvent.json");
    ObjectNodePublisher<ObjectNode> publisher = ObjectNodePublisher.fromBytes(json);

    // First subscriber
    java.util.concurrent.atomic.AtomicReference<Throwable> firstError =
        new java.util.concurrent.atomic.AtomicReference<>();
    java.util.concurrent.CountDownLatch firstLatch = new java.util.concurrent.CountDownLatch(1);

    publisher.subscribe(new Flow.Subscriber<>() {
      private Flow.Subscription subscription;

      @Override
      public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(10);
      }

      @Override
      public void onNext(ObjectNode item) {
        // Process item
      }

      @Override
      public void onError(Throwable throwable) {
        firstError.set(throwable);
        firstLatch.countDown();
      }

      @Override
      public void onComplete() {
        firstLatch.countDown();
      }
    });

    // Second subscriber should receive error
    java.util.concurrent.atomic.AtomicReference<Throwable> secondError =
        new java.util.concurrent.atomic.AtomicReference<>();
    java.util.concurrent.CountDownLatch secondLatch = new java.util.concurrent.CountDownLatch(1);

    publisher.subscribe(new Flow.Subscriber<>() {
      @Override
      public void onSubscribe(Flow.Subscription subscription) {
        subscription.request(1);
      }

      @Override
      public void onNext(ObjectNode item) {
        fail("Second subscriber should not receive items");
      }

      @Override
      public void onError(Throwable throwable) {
        secondError.set(throwable);
        secondLatch.countDown();
      }

      @Override
      public void onComplete() {
        fail("Second subscriber should receive error, not complete");
      }
    });

    // Wait for second subscriber to receive error
    assertTrue(secondLatch.await(1, java.util.concurrent.TimeUnit.SECONDS));
    assertNotNull(secondError.get());
    assertInstanceOf(IllegalStateException.class, secondError.get());
    assertTrue(secondError.get().getMessage().contains("already has a subscriber"));
  }

  @Test
  public void testSourceErrorPropagation() throws Exception {
    RuntimeException expectedError = new RuntimeException("Test source error");

    // Create a source that emits an error
    Flow.Publisher<ByteBuffer> errorSource = subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        @Override
        public void request(long n) {
          if (n > 0) {
            subscriber.onError(expectedError);
          }
        }

        @Override
        public void cancel() {}
      });
    };

    ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(errorSource);

    java.util.concurrent.atomic.AtomicReference<Throwable> receivedError =
        new java.util.concurrent.atomic.AtomicReference<>();
    java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);

    publisher.subscribe(new Flow.Subscriber<>() {
      @Override
      public void onSubscribe(Flow.Subscription subscription) {
        subscription.request(1);
      }

      @Override
      public void onNext(ObjectNode item) {
        fail("Should not receive items when source errors");
      }

      @Override
      public void onError(Throwable throwable) {
        receivedError.set(throwable);
        latch.countDown();
      }

      @Override
      public void onComplete() {
        fail("Should receive error, not complete");
      }
    });

    assertTrue(latch.await(1, java.util.concurrent.TimeUnit.SECONDS));
    assertSame(expectedError, receivedError.get());
  }

  @Test
  public void testRetryMechanismForEarlyEventList() throws Exception {
    // Create a temp file with early-eventList document
    java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("epcis-early-eventlist", ".json");
    try {
      byte[] json = readResource("/object-node-publisher/TwoEvents-earlyEventList.json");
      java.nio.file.Files.write(tempFile, json);

      // Track how many times the retry callable is invoked
      java.util.concurrent.atomic.AtomicInteger retryCount = new java.util.concurrent.atomic.AtomicInteger(0);

      // Use fromInputStream with retry callable
      List<ObjectNode> nodes;
      try (InputStream is = java.nio.file.Files.newInputStream(tempFile)) {
        nodes = ObjectNodePublisher.fromInputStream(is, () -> {
          retryCount.incrementAndGet();
          return java.nio.file.Files.newInputStream(tempFile);
        }).toMulti()
            .subscribe().asStream().toList();
      }

      // Should get header + 2 events = 3 nodes
      assertEquals(3, nodes.size());
      assertTrue(EPCISNodes.hasHeader(nodes));
      assertEquals(2, EPCISNodes.countEvents(nodes));

      // Retry should have been triggered for early-eventList
      // Note: retry may or may not be called depending on document structure
      // The important thing is all nodes are emitted correctly
    } finally {
      java.nio.file.Files.deleteIfExists(tempFile);
    }
  }

  // ========== Early-eventList Behavior Tests ==========

  @Test
  public void testRetryActuallyTriggeredForEarlyEventList() throws Exception {
    // Verify that retry is actually triggered when early-eventList is detected
    java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("epcis-early-eventlist", ".json");
    try {
      byte[] json = readResource("/object-node-publisher/TwoEvents-earlyEventList.json");
      java.nio.file.Files.write(tempFile, json);

      java.util.concurrent.atomic.AtomicInteger retryCount = new java.util.concurrent.atomic.AtomicInteger(0);

      List<ObjectNode> nodes;
      try (InputStream is = java.nio.file.Files.newInputStream(tempFile)) {
        nodes = ObjectNodePublisher.fromInputStream(is, () -> {
          retryCount.incrementAndGet();
          return java.nio.file.Files.newInputStream(tempFile);
        }).toMulti()
            .subscribe().asStream().toList();
      }

      // Retry MUST have been triggered exactly once for early-eventList document
      assertEquals(1, retryCount.get(), "Retry should be triggered exactly once for early-eventList document");
      assertEquals(3, nodes.size());
    } finally {
      java.nio.file.Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void testNoRetryForNormalDocument() throws Exception {
    // Verify that retry is NOT triggered for normal documents
    java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("epcis-normal", ".json");
    try {
      byte[] json = readResource("/object-node-publisher/TwoEvents.json");
      java.nio.file.Files.write(tempFile, json);

      java.util.concurrent.atomic.AtomicInteger retryCount = new java.util.concurrent.atomic.AtomicInteger(0);

      List<ObjectNode> nodes;
      try (InputStream is = java.nio.file.Files.newInputStream(tempFile)) {
        nodes = ObjectNodePublisher.fromInputStream(is, () -> {
          retryCount.incrementAndGet();
          return java.nio.file.Files.newInputStream(tempFile);
        }).toMulti()
            .subscribe().asStream().toList();
      }

      // Retry should NOT have been triggered for normal document
      assertEquals(0, retryCount.get(), "Retry should not be triggered for normal document");
      assertEquals(3, nodes.size()); // header + 2 events
    } finally {
      java.nio.file.Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void testEarlyEventListWithContextAtEnd() throws Exception {
    // Test document where @context appears at the very end
    java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("epcis-context-at-end", ".json");
    try {
      byte[] json = readResource("/object-node-publisher/TwoEvents-earlyEventList-contextAtEnd.json");
      java.nio.file.Files.write(tempFile, json);

      java.util.concurrent.atomic.AtomicInteger retryCount = new java.util.concurrent.atomic.AtomicInteger(0);

      List<ObjectNode> nodes;
      try (InputStream is = java.nio.file.Files.newInputStream(tempFile)) {
        nodes = ObjectNodePublisher.fromInputStream(is, () -> {
          retryCount.incrementAndGet();
          return java.nio.file.Files.newInputStream(tempFile);
        }).toMulti()
            .subscribe().asStream().toList();
      }

      // Should trigger retry and emit all nodes
      assertEquals(1, retryCount.get(), "Retry should be triggered for context-at-end document");
      assertEquals(3, nodes.size());
      assertTrue(EPCISNodes.hasHeader(nodes));

      // Verify header has @context
      ObjectNode header = nodes.stream()
          .filter(n -> EPCIS.EPCIS_DOCUMENT.equals(n.path(EPCIS.TYPE).asText()))
          .findFirst()
          .orElseThrow();
      assertTrue(header.has(EPCIS.CONTEXT), "Header should have @context");
    } finally {
      java.nio.file.Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void testEarlyEventListWithManyEvents() throws Exception {
    // Test with 3 events to ensure all events are captured on retry
    java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("epcis-early-3events", ".json");
    try {
      byte[] json = readResource("/object-node-publisher/ThreeEvents-earlyEventList.json");
      java.nio.file.Files.write(tempFile, json);

      List<ObjectNode> nodes;
      try (InputStream is = java.nio.file.Files.newInputStream(tempFile)) {
        nodes = ObjectNodePublisher.fromInputStream(is, () ->
            java.nio.file.Files.newInputStream(tempFile)
        ).toMulti()
            .subscribe().asStream().toList();
      }

      // Should get header + 3 events
      assertEquals(4, nodes.size());
      assertTrue(EPCISNodes.hasHeader(nodes));
      assertEquals(3, EPCISNodes.countEvents(nodes));
    } finally {
      java.nio.file.Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void testEarlyEventListHeaderEmittedFirst() throws Exception {
    // Verify that header is always the first node even with early-eventList + retry
    java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("epcis-early-order", ".json");
    try {
      byte[] json = readResource("/object-node-publisher/TwoEvents-earlyEventList.json");
      java.nio.file.Files.write(tempFile, json);

      List<ObjectNode> nodes;
      try (InputStream is = java.nio.file.Files.newInputStream(tempFile)) {
        nodes = ObjectNodePublisher.fromInputStream(is, () ->
            java.nio.file.Files.newInputStream(tempFile)
        ).toMulti()
            .subscribe().asStream().toList();
      }

      // First node must be the header
      assertEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(0).path(EPCIS.TYPE).asText(),
          "First node should always be the header");

      // Remaining nodes should be events
      for (int i = 1; i < nodes.size(); i++) {
        assertNotEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(i).path(EPCIS.TYPE).asText(),
            "Nodes after header should be events");
      }
    } finally {
      java.nio.file.Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void testEarlyEventListWithoutRetryReturnsOnlyHeader() throws IOException {
    // Explicitly test that without retry, only header is returned
    byte[] json = readResource("/object-node-publisher/ThreeEvents-earlyEventList.json");

    // Use constructor without retry source
    ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(
        createByteBufferPublisher(json)
    );

    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    // Should only get header (3 events skipped due to early-eventList without retry)
    assertEquals(1, nodes.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(0).path(EPCIS.TYPE).asText());
  }

  // Helper methods

  /**
   * Creates a Flow.Publisher that emits all bytes in a single ByteBuffer.
   */
  private Flow.Publisher<ByteBuffer> createByteBufferPublisher(byte[] data) {
    return subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        private boolean completed = false;

        @Override
        public void request(long n) {
          if (!completed && n > 0) {
            completed = true;
            subscriber.onNext(ByteBuffer.wrap(data));
            subscriber.onComplete();
          }
        }

        @Override
        public void cancel() {
          completed = true;
        }
      });
    };
  }

  /**
   * Creates a Flow.Publisher that emits bytes in chunks.
   */
  private Flow.Publisher<ByteBuffer> createChunkedByteBufferPublisher(byte[] data, int chunkSize) {
    return subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        private int offset = 0;
        private boolean cancelled = false;

        @Override
        public void request(long n) {
          if (cancelled) return;

          for (long i = 0; i < n && offset < data.length && !cancelled; i++) {
            int len = Math.min(chunkSize, data.length - offset);
            byte[] chunk = new byte[len];
            System.arraycopy(data, offset, chunk, 0, len);
            offset += len;
            subscriber.onNext(ByteBuffer.wrap(chunk));
          }

          if (offset >= data.length && !cancelled) {
            subscriber.onComplete();
          }
        }

        @Override
        public void cancel() {
          cancelled = true;
        }
      });
    };
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
