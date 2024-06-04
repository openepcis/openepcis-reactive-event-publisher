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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.Flow;
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowPublisherVerification;

public class ObjectNodePublisherVerificationTest extends FlowPublisherVerification<ObjectNode> {

  private final ObjectMapper objectMapper =
      new ObjectMapper()
          .setSerializationInclusion(JsonInclude.Include.NON_NULL)
          .registerModule(new Jdk8Module())
          .registerModule(new JavaTimeModule())
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false);

  public ObjectNodePublisherVerificationTest() {
    super(new TestEnvironment());
  }

  @Override
  public Flow.Publisher<ObjectNode> createFlowPublisher(long l) {
    try {
      if (l == 0) {
        return new ObjectNodePublisher<>(new StringReader("{}"));
      }
      final ObjectNode json =
          (ObjectNode)
              objectMapper.readTree(
                  getClass().getResourceAsStream("/object-node-publisher/ThreeEvents.json"));
      final ArrayNode eventList = (ArrayNode) json.get("epcisBody").get("eventList");
      // eventList.size() + 1
      // document node also has to be considered
      while (l < eventList.size() + 1) {
        eventList.remove(0);
      }
      // prevent huge event lists, max is 16
      while (l > eventList.size() + 1 && eventList.size() < 16) {
        eventList.add(eventList.get(0).deepCopy());
      }
      final String doc = objectMapper.writeValueAsString(json);
      return new ObjectNodePublisher(new StringReader(doc));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Flow.Publisher<ObjectNode> createFailedFlowPublisher() {
    try {
      return new ObjectNodePublisher(
          getClass().getResourceAsStream("/object-node-publisher/ThrowError.json"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
