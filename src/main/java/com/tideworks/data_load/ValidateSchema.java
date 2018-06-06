package com.tideworks.data_load;

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

class ValidateSchema {
  static final String csvDelimiter = ",";

  static void validate(final File schemaFile) throws IOException {
    final Schema arvoSchema = new Schema.Parser().setValidate(true).parse(schemaFile);
    final List<String> fieldNames = arvoSchema.getFields().stream()
            .map(field -> field.name().toUpperCase())
            .collect(Collectors.toList());
    System.err.println(String.join(csvDelimiter, fieldNames.toArray(new String[0])));
  }
}