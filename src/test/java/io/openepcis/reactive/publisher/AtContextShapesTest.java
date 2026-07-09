package io.openepcis.reactive.publisher;
import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Multi;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Robustness: the parser must emit header + all events for every valid @context shape. */
public class AtContextShapesTest {
  private static final String U = "https://ref.gs1.org/standards/epcis/epcis-context.jsonld";
  private static final String EV =
      "{\"type\":\"ObjectEvent\",\"eventTime\":\"2026-07-09T00:00:00.000Z\",\"eventTimeZoneOffset\":\"+00:00\","
    + "\"action\":\"ADD\",\"bizStep\":\"commissioning\",\"epcList\":[\"https://id.x/01/09521000001428/21/S1\"],\"oec:dppStatus\":\"active\"}";

  private String doc(String ctx) {
    return "{\"@context\":" + ctx + ",\"type\":\"EPCISDocument\",\"schemaVersion\":\"2.0\","
      + "\"creationDate\":\"2026-07-09T00:00:00.000Z\",\"epcisBody\":{\"eventList\":[" + EV + "]}}";
  }
  private int nodes(String json) throws Exception {
    List<ObjectNode> n = Multi.createFrom()
        .publisher(ObjectNodePublisher.fromInputStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))))
        .subscribe().asStream().toList();
    return n.size();
  }
  private void check(String label, String ctx) throws Exception {
    int n = nodes(doc(ctx));
    System.out.println("  "+label+" -> nodes="+n);
    assertEquals(2, n, label+": expected header + 1 event");
  }

  @Test public void allShapes() throws Exception {
    check("bare-string",            "\""+U+"\"");
    check("array-1-string",         "[\""+U+"\"]");
    check("array-string+obj",       "[\""+U+"\",{\"oec\":\"https://ref.openepcis.io/extensions/common/core/\"}]");
    check("obj-with-contextUrls",   "[\""+U+"\",{\"oec\":\"https://ref.openepcis.io/extensions/common/core/\",\"contextUrls\":[\""+U+"\"]}]");
    check("bare-object",            "{\"oec\":\"https://ref.openepcis.io/extensions/common/core/\"}");
    check("two-objects",            "[\""+U+"\",{\"oec\":\"https://a/\"},{\"eubat\":\"https://b/\"}]");
    check("obj-nested-object",      "[\""+U+"\",{\"oec\":\"https://a/\",\"meta\":{\"k\":\"v\"}}]");
    check("obj-nested-array-array", "[\""+U+"\",{\"oec\":\"https://a/\",\"lists\":[[\"x\"],[\"y\"]]}]");
  }
}
