package com.tideworks.data_load.util;

import org.apache.avro.Schema;
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.deser.std.StdDeserializer;

import java.io.IOException;

public final class JsonAvroSchemaSerializer {

  public static final class AvroSchemaSerializer extends JsonSerializer<Schema> {
    @SuppressWarnings("DuplicateThrows")
    @Override
    public void serialize(Schema schema, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
          throws IOException, JsonProcessingException
    {
      jsonGenerator.writeRawValue(schema.toString(true));
    }
  }

  @SuppressWarnings("unused")
  public static final class AvroSchemaDeserializer extends StdDeserializer<Schema> {
    public AvroSchemaDeserializer() { super(Schema.class); }

    protected AvroSchemaDeserializer(Class<?> vc) {
      super(vc);
    }

    @SuppressWarnings("DuplicateThrows")
    @Override
    public Schema deserialize(JsonParser p, DeserializationContext deserializationContext)
          throws IOException, JsonProcessingException
    {
      final ObjectCodec oc = p.getCodec();
      final JsonNode node = oc.readTree(p);
      final String textValue = node.toString();
      return new Schema.Parser().setValidate(true).parse(textValue);
    }
  }
}