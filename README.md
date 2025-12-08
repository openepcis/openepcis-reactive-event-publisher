# openepcis-reactive-event-publisher

Reactive streaming parser for EPCIS 2.0 JSON documents. Provides `ObjectNodePublisher` for non-blocking parsing of EPCIS events with full Reactive Streams backpressure support and TCK compliance.

## Features

- **Non-blocking JSON parsing** using Jackson's NonBlockingJsonParser
- **Full backpressure support** - stops requesting input when subscriber demand is satisfied
- **Reactive Streams TCK compliant** - verified against the official test compatibility kit
- **Support for early-eventList documents** via automatic retry mechanism
- **Compatible with Mutiny Multi** and other reactive frameworks (Project Reactor, RxJava)
- **Netty ByteBuf support** for zero-copy integration with Quarkus/Vert.x
- **Minimal dependencies** - no logging framework required (uses JDK Platform Logging)

## Installation

```xml
<dependency>
    <groupId>io.openepcis</groupId>
    <artifactId>openepcis-reactive-event-publisher</artifactId>
    <version>${openepcis.version}</version>
</dependency>
```

## Usage

### From InputStream (most common)

```java
// Simple usage
ObjectNodePublisher.fromInputStream(inputStream)
    .toMulti()
    .subscribe().with(node -> process(node));

// With custom buffer size
ObjectNodePublisher.fromInputStream(inputStream, 16384)
    .toMulti()
    .subscribe().with(node -> process(node));

// With retry support for early-eventList documents
ObjectNodePublisher.fromInputStream(inputStream, () -> new FileInputStream("data.json"))
    .toMulti()
    .subscribe().with(node -> process(node));
```

### From byte array

```java
// Blocking convenience method
byte[] json = Files.readAllBytes(path);
List<ObjectNode> nodes = ObjectNodePublisher.parseAll(json);

// Reactive
ObjectNodePublisher.fromBytes(json)
    .toMulti()
    .filter(EPCISNodes::isEvent)
    .subscribe().with(event -> process(event));
```

### From reactive ByteBuffer source (HTTP streaming)

```java
// From any Flow.Publisher<ByteBuffer>
Flow.Publisher<ByteBuffer> httpBody = ...;
ObjectNodePublisher.from(httpBody)
    .toMulti()
    .subscribe().with(node -> process(node));

// From Mutiny Multi
Multi<ByteBuffer> body = ...;
ObjectNodePublisher.fromMulti(body)
    .toMulti()
    .subscribe().with(node -> process(node));
```

### Using the Builder

```java
// With InputStream
ObjectNodePublisher.builder()
    .inputStream(myInputStream)
    .bufferSize(16384)  // optional, default 8192
    .retryInputStream(() -> new FileInputStream("data.json"))
    .build();

// With reactive source
ObjectNodePublisher.builder()
    .source(byteBufferPublisher)
    .retrySource(() -> createNewPublisher())
    .build();
```

### Quarkus REST endpoint example

```java
@POST
@Path("/events")
@Consumes(MediaType.APPLICATION_JSON)
public Multi<String> processEvents(Multi<ByteBuffer> body) throws IOException {
    return ObjectNodePublisher.fromMulti(body)
        .toMulti()
        .filter(EPCISNodes::isEvent)
        .map(event -> EPCISNodes.getType(event) + ": " + EPCISNodes.getEventTime(event));
}
```

## Working with parsed nodes

The `EPCISNodes` utility class provides convenient methods:

```java
List<ObjectNode> nodes = ObjectNodePublisher.parseAll(json);

// Extract header and events
ObjectNode header = EPCISNodes.header(nodes).orElseThrow();
List<ObjectNode> events = EPCISNodes.events(nodes);

// Filter by event type
List<ObjectNode> objectEvents = EPCISNodes.filterByType(nodes, "ObjectEvent");

// Check node types
if (EPCISNodes.isHeader(node)) { ... }
if (EPCISNodes.isEvent(node)) { ... }

// Get common fields
String eventTime = EPCISNodes.getEventTime(event);
String bizStep = EPCISNodes.getBizStep(event);
String action = EPCISNodes.getAction(event);

// Counts and checks
long eventCount = EPCISNodes.countEvents(nodes);
boolean hasEvents = EPCISNodes.hasEvents(nodes);
boolean hasHeader = EPCISNodes.hasHeader(nodes);
```

## Netty ByteBuf Support

For Netty-based applications (Quarkus, Vert.x):

```java
import io.openepcis.reactive.publisher.ByteBufSupport;

// From Mutiny Multi<ByteBuf>
Multi<ByteBuf> nettyBody = ...;
ByteBufSupport.fromMulti(nettyBody)
    .toMulti()
    .subscribe().with(node -> process(node));

// Quick one-liner (auto-releases ByteBuf)
List<ObjectNode> nodes = ByteBufSupport.parseAll(byteBuf);

// Check if Netty is available
if (ByteBufSupport.isAvailable()) {
    // Netty is on classpath
}
```

**Note:** Netty dependency is optional. Methods throw `NoClassDefFoundError` if Netty is not on classpath.

## The Early-EventList Problem

EPCIS JSON documents should have `@context` before `epcisBody.eventList`, but some producers emit them in different order:

```json
// PROBLEM: eventList appears before @context
{"epcisBody":{"eventList":[...]}, "@context":[...], "type":"EPCISDocument"}
```

When this happens, provide a retry callable to handle the second pass:

```java
ObjectNodePublisher.fromInputStream(
    inputStream,
    () -> new FileInputStream("same-file.json")  // Retry for second pass
);
```

Without retry callable: early-eventList documents emit only the header, no events.

## Architecture

```
io.openepcis.reactive.publisher/
├── ObjectNodePublisher.java      # Main reactive publisher (Flow.Publisher<ObjectNode>)
├── AsyncObjectNodeParser.java    # Non-blocking JSON parser using Jackson
├── EPCISNodes.java               # Utilities for working with parsed nodes
├── ByteBufSupport.java           # Netty ByteBuf adapter (optional dependency)
└── ObjectNodeUtil.java           # Internal validation utilities
```

## Thread Safety

Publishers are designed for single-threaded use. Subscribe and consume from one thread only. The publisher handles backpressure correctly - it will pause upstream requests when downstream demand is exhausted.

## Emission Order

1. **Header** - EPCISDocument ObjectNode (must have `type`, `@context`, `schemaVersion`, `creationDate`)
2. **Events** - Each event as separate ObjectNode (with `type` like `ObjectEvent`, `AggregationEvent`, etc.)

## License

Apache License 2.0
