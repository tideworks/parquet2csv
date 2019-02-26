package com.tideworks.data_load.util;

import org.codehaus.jackson.*;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.map.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class JsonStrMapSerializer {
  public static final String avroSchemaFieldName = "parquet.avro.schema";

  public static final class StringMapSerializer extends JsonSerializer<Map<String, String>> {
    @SuppressWarnings("DuplicateThrows")
    @Override
    public void serialize(Map<String, String> strMap, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider) throws IOException, JsonProcessingException
    {
      jsonGenerator.writeStartObject();
      if (!strMap.isEmpty()) {
        for(final Map.Entry<String, String> entry : strMap.entrySet()) {
          final String fieldName = entry.getKey();
          if (fieldName.equals(avroSchemaFieldName)) {
            final String jsonText = entry.getValue().replace("\\\"", "\"");
            final JsonNode jsonNode = new ObjectMapper().readTree(jsonText);
            jsonGenerator.writeObjectField(fieldName, jsonNode);
          } else {
            jsonGenerator.writeStringField(fieldName, entry.getValue());
          }
        }
      }
      jsonGenerator.writeEndObject();
    }
  }

  public static final class StringMapDeserializer extends StdDeserializer<Map<String, String>> {
    public StringMapDeserializer() {
      super(Map.class);
    }

    protected StringMapDeserializer(Class<?> vc) {
      super(vc);
    }

    @SuppressWarnings({"DuplicateThrows", "unchecked"})
    @Override
    public Map<String, String> deserialize(JsonParser p, DeserializationContext deserializationContext)
          throws IOException, JsonProcessingException
    {
      final ObjectCodec oc = p.getCodec();
      final JsonNode node = oc.readTree(p);
      final Iterator<String> fieldNames = node.getFieldNames();
      final Map<String, String> map = new LinkedHashMap<>();
      while (fieldNames.hasNext()) {
        final String fieldName = fieldNames.next();
        final String textValue = fieldName.equals(avroSchemaFieldName)
                                       ? node.get(avroSchemaFieldName).toString() : node.get(fieldName).getTextValue();
        map.put(fieldName, textValue);
      }
      return map.isEmpty() ? (Map<String, String>) Collections.EMPTY_MAP : map;
    }
  }
}