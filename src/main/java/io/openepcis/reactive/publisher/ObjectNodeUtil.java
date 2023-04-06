package io.openepcis.reactive.publisher;

import static io.openepcis.constants.EPCIS.REQUIRED_DOCUMENT_FIELDS;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ObjectNodeUtil {

  public static boolean isValidEPCISDocumentNode(final ObjectNode header) {
    for (String field : REQUIRED_DOCUMENT_FIELDS) {
      if (!header.has(field)) {
        return false;
      }
    }
    return true;
  }
}
