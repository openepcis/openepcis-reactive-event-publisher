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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility methods for working with parsed EPCIS ObjectNodes.
 *
 * <p>Provides convenient filtering, extraction, and type-checking methods
 * for EPCIS document headers and events.
 *
 * <p><strong>Example usage:</strong>
 * <pre>{@code
 * List<ObjectNode> nodes = ObjectNodePublisher.parseAll(json);
 *
 * // Extract header and events
 * ObjectNode header = EPCISNodes.header(nodes).orElseThrow();
 * List<ObjectNode> events = EPCISNodes.events(nodes);
 *
 * // Filter by event type
 * List<ObjectNode> objectEvents = EPCISNodes.filterByType(nodes, "ObjectEvent");
 *
 * // Check node types
 * if (EPCISNodes.isHeader(node)) { ... }
 * if (EPCISNodes.isEvent(node)) { ... }
 * }</pre>
 */
public final class EPCISNodes {

  private EPCISNodes() {
    // Utility class
  }

  /**
   * Checks if a node is an EPCIS document header (EPCISDocument or EPCISQueryDocument).
   *
   * @param node the node to check
   * @return true if this is a document header
   */
  public static boolean isHeader(ObjectNode node) {
    if (node == null) return false;
    String type = node.path(TYPE).asText();
    return EPCIS_DOCUMENT.equals(type) || EPCIS_QUERY_DOCUMENT.equals(type);
  }

  /**
   * Checks if a node is an EPCIS event (has type field but not a document type).
   *
   * @param node the node to check
   * @return true if this is an event
   */
  public static boolean isEvent(ObjectNode node) {
    if (node == null) return false;
    String type = node.path(TYPE).asText();
    return type != null && !type.isEmpty()
        && !EPCIS_DOCUMENT.equals(type)
        && !EPCIS_QUERY_DOCUMENT.equals(type);
  }

  /**
   * Checks if a node is a specific event type.
   *
   * @param node the node to check
   * @param eventType the event type (e.g., "ObjectEvent", "AggregationEvent")
   * @return true if node is an event of the specified type
   */
  public static boolean isEventType(ObjectNode node, String eventType) {
    if (node == null || eventType == null) return false;
    return eventType.equals(node.path(TYPE).asText());
  }

  /**
   * Extracts the document header from a list of nodes.
   *
   * @param nodes list of parsed nodes
   * @return the header node, or empty if not found
   */
  public static Optional<ObjectNode> header(List<ObjectNode> nodes) {
    if (nodes == null) return Optional.empty();
    return nodes.stream().filter(EPCISNodes::isHeader).findFirst();
  }

  /**
   * Extracts all events from a list of nodes (excludes header).
   *
   * @param nodes list of parsed nodes
   * @return list of event nodes
   */
  public static List<ObjectNode> events(List<ObjectNode> nodes) {
    return eventsStream(nodes).collect(Collectors.toList());
  }

  /**
   * Returns a lazy stream of events from a list of nodes (excludes header).
   *
   * <p>Use this for lazy evaluation when you don't need all events at once,
   * or when chaining with other stream operations.
   *
   * <p><strong>Example:</strong>
   * <pre>{@code
   * EPCISNodes.eventsStream(nodes)
   *     .filter(e -> "shipping".equals(EPCISNodes.getBizStep(e)))
   *     .findFirst()
   *     .ifPresent(this::process);
   * }</pre>
   *
   * @param nodes list of parsed nodes
   * @return stream of event nodes (empty stream if nodes is null)
   */
  public static Stream<ObjectNode> eventsStream(List<ObjectNode> nodes) {
    if (nodes == null) return Stream.empty();
    return nodes.stream().filter(EPCISNodes::isEvent);
  }

  /**
   * Filters nodes by event type.
   *
   * @param nodes list of parsed nodes
   * @param eventType the event type to filter by (e.g., "ObjectEvent")
   * @return list of matching events
   */
  public static List<ObjectNode> filterByType(List<ObjectNode> nodes, String eventType) {
    return filterByTypeStream(nodes, eventType).collect(Collectors.toList());
  }

  /**
   * Returns a lazy stream of nodes filtered by event type.
   *
   * @param nodes list of parsed nodes
   * @param eventType the event type to filter by (e.g., "ObjectEvent")
   * @return stream of matching events (empty stream if nodes or eventType is null)
   */
  public static Stream<ObjectNode> filterByTypeStream(List<ObjectNode> nodes, String eventType) {
    if (nodes == null || eventType == null) return Stream.empty();
    return nodes.stream().filter(n -> eventType.equals(n.path(TYPE).asText()));
  }

  /**
   * Filters nodes by a custom predicate.
   *
   * @param nodes list of parsed nodes
   * @param predicate the filter predicate
   * @return list of matching nodes
   */
  public static List<ObjectNode> filter(List<ObjectNode> nodes, Predicate<ObjectNode> predicate) {
    return filterStream(nodes, predicate).collect(Collectors.toList());
  }

  /**
   * Returns a lazy stream of nodes filtered by a custom predicate.
   *
   * @param nodes list of parsed nodes
   * @param predicate the filter predicate
   * @return stream of matching nodes (empty stream if nodes or predicate is null)
   */
  public static Stream<ObjectNode> filterStream(List<ObjectNode> nodes, Predicate<ObjectNode> predicate) {
    if (nodes == null || predicate == null) return Stream.empty();
    return nodes.stream().filter(predicate);
  }

  /**
   * Gets the event type from a node.
   *
   * @param node the node
   * @return the type value, or empty string if not present
   */
  public static String getType(ObjectNode node) {
    if (node == null) return "";
    return node.path(TYPE).asText("");
  }

  /**
   * Gets the eventTime from an event node.
   *
   * @param node the event node
   * @return the eventTime value, or empty string if not present
   */
  public static String getEventTime(ObjectNode node) {
    if (node == null) return "";
    return node.path(EVENT_TIME).asText("");
  }

  /**
   * Gets the bizStep from an event node.
   *
   * @param node the event node
   * @return the bizStep value, or empty string if not present
   */
  public static String getBizStep(ObjectNode node) {
    if (node == null) return "";
    return node.path(BIZ_STEP).asText("");
  }

  /**
   * Gets the action from an event node (for ObjectEvent, AggregationEvent, etc.).
   *
   * @param node the event node
   * @return the action value, or empty string if not present
   */
  public static String getAction(ObjectNode node) {
    if (node == null) return "";
    return node.path(ACTION).asText("");
  }

  /**
   * Counts the number of events in a list of nodes.
   *
   * @param nodes list of parsed nodes
   * @return count of events (excludes header)
   */
  public static long countEvents(List<ObjectNode> nodes) {
    if (nodes == null) return 0;
    return nodes.stream().filter(EPCISNodes::isEvent).count();
  }

  /**
   * Checks if the list contains any events.
   *
   * @param nodes list of parsed nodes
   * @return true if at least one event is present
   */
  public static boolean hasEvents(List<ObjectNode> nodes) {
    if (nodes == null) return false;
    return nodes.stream().anyMatch(EPCISNodes::isEvent);
  }

  /**
   * Checks if the list contains a valid header.
   *
   * @param nodes list of parsed nodes
   * @return true if a header is present
   */
  public static boolean hasHeader(List<ObjectNode> nodes) {
    if (nodes == null) return false;
    return nodes.stream().anyMatch(EPCISNodes::isHeader);
  }
}
