package com.tideworks.data_load.util;

import org.apache.avro.Schema;
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
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
      final String jsonText = schema.toString(); // serializes Avro schema object to JSON representation
      final JsonNode jsonNode = new ObjectMapper().readTree(jsonText); // parse JSON into Jackson JSON runtime object
      jsonGenerator.writeObject(jsonNode); // let Jackson write the JSON runtime object into the JSON output stream
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
      final String jsonText = node.toString();
      return new Schema.Parser().setValidate(true).parse(jsonText); // de-serialize Avro schema into runtime object
    }
  }
}