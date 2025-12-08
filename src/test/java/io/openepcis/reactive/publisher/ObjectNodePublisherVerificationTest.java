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
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowPublisherVerification;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Flow;

/**
 * Reactive Streams TCK verification test for ObjectNodePublisher.
 *
 * <p>This test verifies that ObjectNodePublisher correctly implements the
 * Java 9+ Flow.Publisher specification, including proper handling of:
 * <ul>
 *   <li>Backpressure signals</li>
 *   <li>Subscription lifecycle</li>
 *   <li>Error propagation</li>
 *   <li>Cancellation</li>
 * </ul>
 */
@Test
public class ObjectNodePublisherVerificationTest extends FlowPublisherVerification<ObjectNode> {

  /**
   * Creates test environment with default timeout settings.
   */
  public ObjectNodePublisherVerificationTest() {
    super(new TestEnvironment(300));
  }

  /**
   * Generates a test EPCIS document with the specified number of events.
   *
   * @param eventCount number of events to include
   * @return JSON string of the EPCIS document
   */
  private String generateEPCISDocument(int eventCount) {
    StringBuilder json = new StringBuilder();
    json.append("{\n");
    json.append("  \"@context\": \"https://ref.gs1.org/standards/epcis/2.0.0/epcis-context.jsonld\",\n");
    json.append("  \"type\": \"EPCISDocument\",\n");
    json.append("  \"schemaVersion\": \"2.0\",\n");
    json.append("  \"creationDate\": \"2025-01-01T00:00:00.000Z\",\n");
    json.append("  \"epcisBody\": {\n");
    json.append("    \"eventList\": [\n");

    for (int i = 0; i < eventCount; i++) {
      if (i > 0) json.append(",\n");
      json.append("      {\n");
      json.append("        \"type\": \"ObjectEvent\",\n");
      json.append("        \"eventTime\": \"2025-01-01T00:00:00.").append(String.format("%03d", i % 1000)).append("Z\",\n");
      json.append("        \"eventTimeZoneOffset\": \"+00:00\",\n");
      json.append("        \"action\": \"ADD\",\n");
      json.append("        \"bizStep\": \"commissioning\"\n");
      json.append("      }");
    }

    json.append("\n    ]\n");
    json.append("  }\n");
    json.append("}\n");
    return json.toString();
  }

  /**
   * Creates a Flow.Publisher that emits the requested number of elements.
   * Elements = 1 header + (elements - 1) events
   */
  @Override
  public Flow.Publisher<ObjectNode> createFlowPublisher(long elements) {
    if (elements <= 0) {
      return createFailedFlowPublisher();
    }

    // Generate document with (elements - 1) events (header counts as 1)
    int eventCount = (int) Math.max(0, elements - 1);
    String json = generateEPCISDocument(eventCount);
    InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));

    try {
      return ObjectNodePublisher.fromInputStream(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create publisher", e);
    }
  }

  /**
   * Creates a publisher that fails immediately after onSubscribe.
   * Per spec109, onError must be signaled after onSubscribe but before any other signal.
   */
  @Override
  public Flow.Publisher<ObjectNode> createFailedFlowPublisher() {
    return subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        @Override
        public void request(long n) {
          // No-op - error already signaled
        }

        @Override
        public void cancel() {
          // No-op
        }
      });
      // Signal error immediately after onSubscribe
      subscriber.onError(new RuntimeException("Failed publisher for TCK test"));
    };
  }

  /**
   * Returns the maximum number of elements that can be produced.
   * We limit this to avoid generating extremely large documents in tests.
   */
  @Override
  public long maxElementsFromPublisher() {
    return 100; // Header + up to 99 events
  }

  /**
   * Indicates the bounded depth of onNext and request recursion.
   */
  @Override
  public long boundedDepthOfOnNextAndRequestRecursion() {
    return 1;
  }
}
