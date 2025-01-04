package io.openepcis.reactive.publisher.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.JsonNodeDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Custom class that extends "JsonNodeDeserializer" to read the JSON information with duplicate keys during the deserialization of the JSON EPCIS events.
 */
@JsonDeserialize(using = JsonNodeDupeFieldHandlingDeserializer.class)
public class JsonNodeDupeFieldHandlingDeserializer extends JsonNodeDeserializer {

    @Override
    protected void _handleDuplicateField(
            final JsonParser p,
            final DeserializationContext context,
            final JsonNodeFactory nodeFactory,
            final String fieldName,
            final ObjectNode objectNode,
            final JsonNode oldValue,
            final JsonNode newValue) {

        // Prepare an ArrayNode to hold values associated with the duplicate field
        ArrayNode asArrayValue;

        // If the existing value is already an array, use it directly
        if (oldValue.isArray()) {
            asArrayValue = (ArrayNode) oldValue;
        } else {
            // Otherwise, create a new ArrayNode and add the existing value to it
            asArrayValue = nodeFactory.arrayNode();
            asArrayValue.add(oldValue);
        }

        // Add the new value to the array
        asArrayValue.add(newValue);

        // Replace the field in the ObjectNode with the updated array
        objectNode.set(fieldName, asArrayValue);
    }
}
