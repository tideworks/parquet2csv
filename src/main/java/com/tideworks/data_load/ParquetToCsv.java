package com.tideworks.data_load;

import com.tideworks.data_load.io.BufferedWriterExt;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import static com.tideworks.data_load.io.InputFile.nioPathToInputFile;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

class ParquetToCsv {
  private static final Logger log = LoggerFactory.getLogger(ParquetToCsv.class.getSimpleName());
  private static final String fileExtent = ".parquet";

  static void processToOutput(final File inputFile) throws IOException {
    final String fileName = inputFile.getName();
    if (!fileName.endsWith(fileExtent)) {
      log.error("\"{}\" does not end in '{}' - thus is not assumed to be a Parquet file and is being skipped",
              inputFile, fileExtent);
      return;
    }
    final int endIndex = fileName.lastIndexOf(fileExtent);
    final String fileNameBase = fileName.substring(0, endIndex);
    final File csvOutputFile = new File(inputFile.getParent(), fileNameBase + ".csv");

    final StringBuilder sb = new StringBuilder(1024);
    final Charset utf8 = Charset.forName("UTF-8");
    final int ioStreamBufSize = 16 * 1024;
    final OutputStream csvOutputStream = Files.newOutputStream(csvOutputFile.toPath(), CREATE, TRUNCATE_EXISTING);
    final Writer outputWriter = new BufferedWriterExt(new OutputStreamWriter(csvOutputStream, utf8), ioStreamBufSize);

    final ThreadLocal<Boolean> validateNames = getFieldValue(Schema.class, null, "validateNames");
    //noinspection ConstantConditions
    validateNames.set(false);

    List<Schema.Field> fields = null;
    String[] fieldNames = new String[0];
    try (final Writer csvOutputWriter = outputWriter;
         final ParquetReader<GenericData.Record> reader = AvroParquetReader
                 .<GenericData.Record>builder(nioPathToInputFile(inputFile.toPath()))
                 .withConf(new Configuration())
                 .build())
    {
      validateNames.set(false);
      GenericData.Record record;
      while ((record = reader.read()) != null) {
        if (fields == null) {
          fields = record.getSchema().getFields();
          fieldNames = getFieldNames(fields);
          final String hdrRow = String.join(ValidateSchema.csvDelimiter, fieldNames);
          csvOutputWriter.write(hdrRow);
          csvOutputWriter.write('\n');
        }
        assert fields != null;
        sb.setLength(0);
        for(final String fieldName : fieldNames) {
          sb.append(record.get(fieldName)).append(ValidateSchema.csvDelimiter);
        }
        sb.deleteCharAt(sb.length() - 1).append('\n');
        csvOutputWriter.append(sb);
        csvOutputWriter.flush();
      }
    }
  }

  private static String[] getFieldNames(List<Schema.Field> fields) {
    final List<String> fieldNames = fields.stream()
            .map(field -> field.name().toUpperCase())
            .collect(Collectors.toList());
    return fieldNames.toArray(new String[0]);
  }

  @SuppressWarnings({"unchecked", "UnusedReturnValue"})
  static <T extends Throwable, R> R uncheckedExceptionThrow(Throwable t) throws T { throw (T) t; }

  @SuppressWarnings({"UnusedReturnValue", "SameParameterValue"})
  private static <T, R> R getFieldValue(final Class<?> cls, final T obj, final String fieldName) {
    try {
      final Field field = cls.getDeclaredField(fieldName);
      assert field != null;
      field.setAccessible(true);
      @SuppressWarnings("unchecked")
      final R rtnObj = (R) (java.lang.reflect.Modifier.isStatic(field.getModifiers()) ? field.get(null) : field.get(obj));
      return rtnObj;
    } catch (NoSuchFieldException|SecurityException|IllegalArgumentException|IllegalAccessException e) {
      uncheckedExceptionThrow(e);
    }
    return null; // will never reach here - hushes compiler
  }
}