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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import static com.tideworks.data_load.io.InputFile.nioPathToInputFile;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

class ParquetToCsv {
  private static final String csvDelimiter = ",";
  private static final Logger log = LoggerFactory.getLogger(ParquetToCsv.class.getSimpleName());
  private static final String fileExtent = ".parquet";
  private static final String notParquetFileErrMsgFmt =
          "\"{}\" does not end in '{}' - thus is not assumed to be a Parquet file and is being skipped";

  static void processToOutput(final File inputFile) throws IOException {
    final String fileName = inputFile.getName();
    if (!fileName.endsWith(fileExtent)) {
      log.error(notParquetFileErrMsgFmt, inputFile, fileExtent);
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

    List<Schema.Field> fields = null;
    String[] fieldNames = new String[0];
    try (final Writer csvOutputWriter = outputWriter;
         final ParquetReader<GenericData.Record> reader = AvroParquetReader
                 .<GenericData.Record>builder(nioPathToInputFile(inputFile.toPath()))
                 .withConf(new Configuration())
                 .build())
    {
      GenericData.Record record;
      while ((record = reader.read()) != null) {
        if (fields == null) {
          fields = record.getSchema().getFields();
          fieldNames = getFieldNames(fields);
          final String hdrRow = String.join(csvDelimiter, fieldNames);
          csvOutputWriter.write(hdrRow);
          csvOutputWriter.write('\n');
        }
        assert fields != null;
        sb.setLength(0);
        for(final String fieldName : fieldNames) {
          sb.append(record.get(fieldName)).append(csvDelimiter);
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
}