/* ParquetMetadataToJsonSerialize.java
 *
 * Copyright June 2019 Tideworks Technology
 * Author: Roger D. Voss
 * MIT License
 */
package com.tideworks.data_load.util.io;

import com.tideworks.data_load.util.JsonAvroSchemaSerializer;
import com.tideworks.data_load.util.JsonStrMapSerializer;
import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class ParquetMetadataToJsonSerialize {

  public static String toPrettyJSON(ParquetMetadata parquetMetadata) throws IOException {
    final org.apache.parquet.hadoop.metadata.FileMetaData prqFMD = parquetMetadata.getFileMetaData();
    final Schema avroSchema = new AvroSchemaConverter().convert(prqFMD.getSchema());
    final FileMetaData custFMD = new FileMetaData(avroSchema, prqFMD.getKeyValueMetaData(), prqFMD.getCreatedBy());
    final StringWriter stringWriter = new StringWriter(4096);
    new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(stringWriter, custFMD);
    return stringWriter.toString();
  }

  @SuppressWarnings("unchecked")
  public static ParquetMetadata fromJSON(Reader jsonReader) throws IOException {
    final FileMetaData custFMD = new ObjectMapper().readValue(jsonReader, FileMetaData.class);
    final MessageType msgType = new AvroSchemaConverter().convert(custFMD.schema);
    final org.apache.parquet.hadoop.metadata.FileMetaData prqFMD =
          new org.apache.parquet.hadoop.metadata.FileMetaData(msgType, custFMD.keyValueMetaData, custFMD.createdBy);
    return new ParquetMetadata(prqFMD, (List<BlockMetaData>) Collections.EMPTY_LIST);
  }

  // class that is an analog to the Parquet library FileMetaData class
  // but the schema field is of type Avro Schema instead of MessageType
  @SuppressWarnings("unused")
  private static final class FileMetaData implements Serializable {
    private static final long serialVersionUID = 1L;
    private Schema schema;
    private Map<String, String> keyValueMetaData;
    private String createdBy;

    public FileMetaData() { this(null, null, null); } // to support serialization

    private FileMetaData(Schema schema, Map<String, String> keyValueMetaData, String createdBy) {
      this.schema = schema;
      this.keyValueMetaData = keyValueMetaData;
      this.createdBy = createdBy;
    }

    @JsonSerialize(using= JsonAvroSchemaSerializer.AvroSchemaSerializer.class)
    public Schema getSchema() { return schema; }
    @JsonDeserialize(using=JsonAvroSchemaSerializer.AvroSchemaDeserializer.class)
    public void setSchema(Schema schema) { this.schema = schema; }

    @JsonSerialize(using= JsonStrMapSerializer.StringMapSerializer.class)
    public Map<String, String> getKeyValueMetaData() { return keyValueMetaData; }
    @JsonDeserialize(using=JsonStrMapSerializer.StringMapDeserializer.class)
    public void setKeyValueMetaData(Map<String, String> keyValueMetaData) { this.keyValueMetaData = keyValueMetaData; }

    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
  }
}
