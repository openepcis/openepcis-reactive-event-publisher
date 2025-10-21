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
import io.openepcis.constants.EPCIS;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance tests for ObjectNodePublisher with large files.
 * These tests validate buffering optimizations for production-scale EPCIS documents.
 */
public class LargeFilePerformanceTest {

  /**
   * Generates a large JSON EPCIS document similar to production files.
   * Simulates a document with thousands of ObjectEvents.
   *
   * @param eventCount number of events to generate
   * @return JSON string representing EPCIS document
   */
  private String generateLargeEPCISDocument(int eventCount) {
    StringBuilder json = new StringBuilder(eventCount * 500); // Pre-size for efficiency

    json.append("{\n");
    json.append("  \"@context\": \"https://ref.gs1.org/standards/epcis/2.0.0/epcis-context.jsonld\",\n");
    json.append("  \"type\": \"EPCISDocument\",\n");
    json.append("  \"schemaVersion\": \"2.0\",\n");
    json.append("  \"creationDate\": \"2025-03-05T08:38:15.000Z\",\n");
    json.append("  \"epcisBody\": {\n");
    json.append("    \"eventList\": [\n");

    for (int i = 0; i < eventCount; i++) {
      if (i > 0) json.append(",\n");

      json.append("      {\n");
      json.append("        \"type\": \"ObjectEvent\",\n");
      json.append("        \"eventTime\": \"2025-02-25T15:36:06.").append(String.format("%03d", i % 1000)).append("Z\",\n");
      json.append("        \"eventTimeZoneOffset\": \"+00:00\",\n");
      json.append("        \"eventID\": \"2025-02-25T15:36:06.").append(i).append("Z-00262970011660206550\",\n");
      json.append("        \"epcList\": [\n");
      json.append("          \"urn:epc:id:sgtin:5000456.007404.").append(String.format("%014d", i)).append("\"\n");
      json.append("        ],\n");
      json.append("        \"action\": \"ADD\",\n");
      json.append("        \"bizStep\": \"commissioning\",\n");
      json.append("        \"disposition\": \"active\",\n");
      json.append("        \"readPoint\": { \"id\": \"urn:epc:id:sgln:5000456.00026.0\" },\n");
      json.append("        \"bizLocation\": { \"id\": \"urn:epc:id:sgln:5000456.00026.0\" }\n");
      json.append("      }");
    }

    json.append("\n    ]\n");
    json.append("  }\n");
    json.append("}\n");

    return json.toString();
  }

  /**
   * Test with VERY SLOW SUBSCRIBER to simulate real backpressure scenario.
   * This is the KEY test - simulates when subscriber processing (e.g., hash generation,
   * validation, DB writes) is MUCH slower than parsing, creating severe backpressure.
   *
   * Without buffering, this causes "cranking" - the publisher waits on slow subscriber
   * while making tons of tiny unbuffered I/O reads, causing severe performance degradation.
   *
   * DISABLED BY DEFAULT - Run manually with: mvn test -Dtest=LargeFilePerformanceTest#testSlowSubscriber_WithBackpressure -DtestSlowSubscriber=true
   * Expected runtime: ~2-3 minutes for 5000 events @ 20ms each
   */
  @Test
  @EnabledIfSystemProperty(named = "testSlowSubscriber", matches = "true")
  public void testSlowSubscriber_WithBackpressure() throws Exception {
    final int eventCount = 5000; // 5000 events × 20ms = ~100 seconds minimum
    final int delayMs = 120000; // Much slower - simulates complex hash generation or DB writes

    System.out.println("\n=== VERY SLOW SUBSCRIBER TEST (simulates SEVERE backpressure) ===");
    System.out.println("Generating EPCIS document with " + eventCount + " events...");
    System.out.println("⚠️  This test will take ~2-3 minutes to run!");

    final String largeJson = generateLargeEPCISDocument(eventCount);
    final long jsonSizeBytes = largeJson.getBytes(StandardCharsets.UTF_8).length;
    System.out.println("Document size: " + (jsonSizeBytes / 1024 / 1024) + " MB");

    final InputStream inputStream = new ByteArrayInputStream(largeJson.getBytes(StandardCharsets.UTF_8));

    System.out.println("\nSimulating VERY SLOW subscriber (" + delayMs + "ms processing time per event)...");
    System.out.println("This simulates complex hash generation, validation, or slow DB writes");
    System.out.println("Expected minimum time: " + (eventCount * delayMs / 1000) + " seconds\n");

    final long startTime = System.currentTimeMillis();
    final int[] processedCount = {0};
    final long[] totalProcessingTime = {0};

    final ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(inputStream);

    // Very slow subscriber that processes items one at a time with significant delay
    Multi.createFrom().publisher(publisher)
        .onItem().transform(item -> {
          processedCount[0]++;
          long itemStart = System.nanoTime();

          // Simulate SLOW processing (complex hash generation, DB writes, network calls)
          try {
            Thread.sleep(delayMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }

          long itemEnd = System.nanoTime();
          totalProcessingTime[0] += (itemEnd - itemStart);

          // Progress indicator every 500 events
          if (processedCount[0] % 500 == 0) {
            long elapsed = (System.currentTimeMillis() - startTime) / 1000;
            System.out.println("  Processed " + processedCount[0] + " events... (" + elapsed + "s elapsed)");
          }

          return item;
        })
        .subscribe()
        .asStream()
        .toList();

    final long endTime = System.currentTimeMillis();
    final long totalDurationMs = endTime - startTime;
    final long avgProcessingMs = (totalProcessingTime[0] / processedCount[0]) / 1_000_000;

    System.out.println("\n=== RESULTS ===");
    System.out.println("Total time: " + (totalDurationMs / 1000) + " seconds (" + totalDurationMs + " ms)");
    System.out.println("Events processed: " + (processedCount[0] - 1) + " (plus 1 document)");
    System.out.println("Avg processing time per event: " + avgProcessingMs + " ms");
    System.out.println("Expected min time (just processing): " + (eventCount * delayMs / 1000) + " seconds");
    System.out.println("Overhead: " + ((totalDurationMs - (eventCount * delayMs)) / 1000) + " seconds");
    System.out.println("\n✅ With buffering, overhead is minimal even with severe backpressure!");

    // Assertions
    assertEquals(eventCount + 1, processedCount[0], "Should process all events");

    // With buffering, overhead should be minimal (< 30% of processing time) even with slow subscriber
    long expectedMinTime = eventCount * delayMs;
    long maxAcceptableTime = (long)(expectedMinTime * 1.3); // 30% overhead acceptable
    assertTrue(totalDurationMs < maxAcceptableTime,
        "With buffered I/O, total time should be close to pure processing time even with severe backpressure. " +
        "Expected: ~" + (expectedMinTime / 1000) + "s, Max: " + (maxAcceptableTime / 1000) + "s, Actual: " + (totalDurationMs / 1000) + "s");
  }

  /**
   * Test parsing a large EPCIS document (10,000 events, ~5MB).
   * This simulates production-scale files with FAST consumption.
   */
  @Test
  public void testLargeDocument_10KEvents_FastConsumption() throws IOException {
    final int eventCount = 10000;
    System.out.println("\n=== FAST CONSUMPTION TEST (baseline) ===");
    System.out.println("Generating EPCIS document with " + eventCount + " events...");

    final String largeJson = generateLargeEPCISDocument(eventCount);
    final long jsonSizeBytes = largeJson.getBytes(StandardCharsets.UTF_8).length;
    System.out.println("Document size: " + (jsonSizeBytes / 1024 / 1024) + " MB (" + jsonSizeBytes + " bytes)");

    final InputStream inputStream = new ByteArrayInputStream(largeJson.getBytes(StandardCharsets.UTF_8));

    System.out.println("Starting parse with ObjectNodePublisher (buffered)...");
    final long startTime = System.currentTimeMillis();

    final ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(inputStream);
    final var result = Multi.createFrom().publisher(publisher).subscribe().asStream().toList();

    final long endTime = System.currentTimeMillis();
    final long durationMs = endTime - startTime;

    System.out.println("Parse completed in " + durationMs + " ms");
    System.out.println("Throughput: " + (jsonSizeBytes / 1024.0 / (durationMs / 1000.0)) + " KB/s");
    System.out.println("Events parsed: " + (result.size() - 1) + " (plus 1 document header)");

    // Assertions
    assertNotNull(result);
    assertEquals(eventCount + 1, result.size(), "Should have document + all events");
    assertEquals(EPCIS.EPCIS_DOCUMENT, result.get(0).get(EPCIS.TYPE).asText(), "First should be document");
    assertEquals("ObjectEvent", result.get(1).get(EPCIS.TYPE).asText(), "Events should be ObjectEvent");

    // Performance assertion - should complete in reasonable time
    assertTrue(durationMs < 30000, "Should parse 10K events in under 30 seconds (actual: " + durationMs + " ms)");
  }

  /**
   * Test parsing a very large EPCIS document (50,000 events, ~25MB).
   * Simulates production files similar to the 22MB BHGR shipment data.
   *
   * This test is disabled by default. Run with: -DtestLargeFile=true
   */
  @Test
  @EnabledIfSystemProperty(named = "testLargeFile", matches = "true")
  public void testVeryLargeDocument_50KEvents() throws IOException {
    final int eventCount = 50000;
    System.out.println("Generating EPCIS document with " + eventCount + " events...");
    System.out.println("This simulates the production 22MB BHGR shipment file...");

    final String largeJson = generateLargeEPCISDocument(eventCount);
    final long jsonSizeBytes = largeJson.getBytes(StandardCharsets.UTF_8).length;
    System.out.println("Document size: " + (jsonSizeBytes / 1024 / 1024) + " MB (" + jsonSizeBytes + " bytes)");

    final InputStream inputStream = new ByteArrayInputStream(largeJson.getBytes(StandardCharsets.UTF_8));

    System.out.println("Starting parse with ObjectNodePublisher (buffered)...");
    final long startTime = System.currentTimeMillis();

    final ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(inputStream);

    // Stream and count without collecting all in memory
    final long[] eventCount_actual = {0};
    Multi.createFrom().publisher(publisher)
        .subscribe().with(
            item -> {eventCount_actual[0]++;
              try {
                Thread.sleep(120000);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            },
            error -> fail("Should not error: " + error.getMessage()),
            () -> System.out.println("Streaming completed")
        );

    final long endTime = System.currentTimeMillis();
    final long durationMs = endTime - startTime;

    System.out.println("Parse completed in " + durationMs + " ms");
    System.out.println("Throughput: " + (jsonSizeBytes / 1024.0 / 1024.0 / (durationMs / 1000.0)) + " MB/s");
    System.out.println("Events parsed: " + (eventCount_actual[0] - 1) + " (plus 1 document header)");

    // Assertions
    assertEquals(eventCount + 1, eventCount_actual[0], "Should have document + all events");

    // Performance assertion - with buffering should be much faster
    assertTrue(durationMs < 60000, "Should parse 50K events in under 60 seconds (actual: " + durationMs + " ms)");

    // Memory efficiency check
    Runtime runtime = Runtime.getRuntime();
    long usedMemoryMB = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;
    System.out.println("Memory used: " + usedMemoryMB + " MB");
    assertTrue(usedMemoryMB < 500, "Should use less than 500MB for streaming (actual: " + usedMemoryMB + " MB)");
  }

  /**
   * Test that buffering optimization doesn't break small files.
   */
  @Test
  public void testSmallDocument_StillWorks() throws IOException {
    final String smallJson = generateLargeEPCISDocument(3);
    final InputStream inputStream = new ByteArrayInputStream(smallJson.getBytes(StandardCharsets.UTF_8));

    final ObjectNodePublisher<ObjectNode> publisher = new ObjectNodePublisher<>(inputStream);
    final var result = Multi.createFrom().publisher(publisher).subscribe().asStream().toList();

    assertNotNull(result);
    assertEquals(4, result.size()); // 1 document + 3 events
  }
}
