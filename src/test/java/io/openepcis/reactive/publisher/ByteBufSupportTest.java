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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.openepcis.constants.EPCIS;
import io.smallrye.mutiny.Multi;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

/**
 * Tests for ByteBufSupport - Netty ByteBuf integration.
 */
public class ByteBufSupportTest {

  @Test
  public void testIsAvailable() {
    // Netty is on the classpath for tests
    assertTrue(ByteBufSupport.isAvailable());
  }

  @Test
  public void testFeedByteBufHeapBuffer() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/OneEvent.json");
    ByteBuf buf = Unpooled.wrappedBuffer(json);

    try {
      assertTrue(buf.hasArray(), "Should have backing array for heap buffer");

      AsyncObjectNodeParser.FeedResult result = ByteBufSupport.feedByteBuf(parser, buf);
      parser.endOfInput();

      // Buffer should be consumed
      assertEquals(0, buf.readableBytes());

      List<ObjectNode> nodes = drainNodes(parser);
      assertEquals(2, nodes.size());
      assertEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(0).get(EPCIS.TYPE).asText());
    } finally {
      buf.release();
    }
  }

  @Test
  public void testFeedByteBufDirectBuffer() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();

    byte[] json = readResource("/object-node-publisher/OneEvent.json");
    ByteBuf buf = Unpooled.directBuffer(json.length);
    buf.writeBytes(json);

    try {
      assertFalse(buf.hasArray(), "Direct buffer should not have backing array");

      AsyncObjectNodeParser.FeedResult result = ByteBufSupport.feedByteBuf(parser, buf);
      parser.endOfInput();

      // Buffer should be consumed
      assertEquals(0, buf.readableBytes());

      List<ObjectNode> nodes = drainNodes(parser);
      assertEquals(2, nodes.size());
    } finally {
      buf.release();
    }
  }

  @Test
  public void testFeedByteBufEmptyBuffer() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();
    ByteBuf buf = Unpooled.EMPTY_BUFFER;

    AsyncObjectNodeParser.FeedResult result = ByteBufSupport.feedByteBuf(parser, buf);
    assertEquals(AsyncObjectNodeParser.FeedResult.NEED_MORE_INPUT, result);
  }

  @Test
  public void testFeedByteBufNullParser() {
    ByteBuf buf = Unpooled.buffer();
    try {
      assertThrows(IllegalArgumentException.class, () ->
          ByteBufSupport.feedByteBuf(null, buf));
    } finally {
      buf.release();
    }
  }

  @Test
  public void testFeedByteBufNullBuffer() throws IOException {
    AsyncObjectNodeParser parser = new AsyncObjectNodeParser();
    assertThrows(IllegalArgumentException.class, () ->
        ByteBufSupport.feedByteBuf(parser, null));
  }

  @Test
  public void testAdaptToByteBuffer() throws IOException {
    byte[] json = readResource("/object-node-publisher/OneEvent.json");

    // Create ByteBuf publisher
    Flow.Publisher<ByteBuf> byteBufSource = subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        private boolean completed = false;

        @Override
        public void request(long n) {
          if (!completed && n > 0) {
            completed = true;
            ByteBuf buf = Unpooled.wrappedBuffer(json);
            subscriber.onNext(buf);
            subscriber.onComplete();
          }
        }

        @Override
        public void cancel() {
          completed = true;
        }
      });
    };

    // Adapt to ByteBuffer
    Flow.Publisher<ByteBuffer> byteBufferSource = ByteBufSupport.adaptToByteBuffer(byteBufSource);

    // Verify we can consume as ByteBuffer
    List<ByteBuffer> buffers = Multi.createFrom().publisher(byteBufferSource)
        .subscribe().asStream().toList();

    assertEquals(1, buffers.size());
    assertEquals(json.length, buffers.get(0).remaining());
  }

  @Test
  public void testAdaptToByteBufferNullSource() {
    assertThrows(IllegalArgumentException.class, () ->
        ByteBufSupport.adaptToByteBuffer(null));
  }

  @Test
  public void testCreatePublisher() throws IOException {
    byte[] json = readResource("/object-node-publisher/TwoEvents.json");

    Flow.Publisher<ByteBuf> source = createByteBufPublisher(json);

    ObjectNodePublisher<ObjectNode> publisher = ByteBufSupport.createPublisher(source);

    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    // Header + 2 events = 3 nodes
    assertEquals(3, nodes.size());
    assertEquals(EPCIS.EPCIS_DOCUMENT, nodes.get(0).get(EPCIS.TYPE).asText());
  }

  @Test
  public void testCreatePublisherWithChunkedInput() throws IOException {
    byte[] json = readResource("/object-node-publisher/ThreeEvents.json");

    // Create chunked ByteBuf publisher
    Flow.Publisher<ByteBuf> source = createChunkedByteBufPublisher(json, 128);

    ObjectNodePublisher<ObjectNode> publisher = ByteBufSupport.createPublisher(source);

    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    // Header + 3 events = 4 nodes
    assertEquals(4, nodes.size());
  }

  @Test
  public void testCreatePublisherNullSource() {
    assertThrows(IllegalArgumentException.class, () ->
        ByteBufSupport.createPublisher(null));
  }

  @Test
  public void testDirectBufferConversion() throws IOException {
    byte[] json = readResource("/object-node-publisher/OneEvent.json");

    // Create publisher with direct ByteBuf
    Flow.Publisher<ByteBuf> source = subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        private boolean completed = false;

        @Override
        public void request(long n) {
          if (!completed && n > 0) {
            completed = true;
            ByteBuf buf = Unpooled.directBuffer(json.length);
            buf.writeBytes(json);
            subscriber.onNext(buf);
            subscriber.onComplete();
          }
        }

        @Override
        public void cancel() {
          completed = true;
        }
      });
    };

    ObjectNodePublisher<ObjectNode> publisher = ByteBufSupport.createPublisher(source);

    List<ObjectNode> nodes = Multi.createFrom().publisher(publisher)
        .subscribe().asStream().toList();

    assertEquals(2, nodes.size());
  }

  // Helper methods

  private Flow.Publisher<ByteBuf> createByteBufPublisher(byte[] data) {
    return subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        private boolean completed = false;

        @Override
        public void request(long n) {
          if (!completed && n > 0) {
            completed = true;
            ByteBuf buf = Unpooled.wrappedBuffer(data);
            subscriber.onNext(buf);
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

  private Flow.Publisher<ByteBuf> createChunkedByteBufPublisher(byte[] data, int chunkSize) {
    return subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        private int offset = 0;
        private boolean cancelled = false;

        @Override
        public void request(long n) {
          if (cancelled) return;

          for (long i = 0; i < n && offset < data.length && !cancelled; i++) {
            int len = Math.min(chunkSize, data.length - offset);
            ByteBuf chunk = Unpooled.wrappedBuffer(data, offset, len);
            offset += len;
            subscriber.onNext(chunk);
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

  private List<ObjectNode> drainNodes(AsyncObjectNodeParser parser) {
    java.util.ArrayList<ObjectNode> nodes = new java.util.ArrayList<>();
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
