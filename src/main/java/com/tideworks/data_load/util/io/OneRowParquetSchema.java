package com.tideworks.data_load.util.io;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.tideworks.data_load.io.InputFile.nioPathToInputFile;
import static com.tideworks.data_load.io.OutputFile.nioPathToOutputFile;
import static com.tideworks.data_load.util.JsonStrMapSerializer.avroSchemaFieldName;
import static com.tideworks.data_load.util.io.FileUtils.makeSchemaFilePathFromBaseFileName;
import static com.tideworks.data_load.util.io.FileUtils.parquetExtent;
import static java.math.RoundingMode.HALF_UP;

public class OneRowParquetSchema {
  private final Path inputFilePath;

  private OneRowParquetSchema(File inputFile) {
    this.inputFilePath = inputFile.toPath();
  }

  public static void writeSchemaFile(final File inputFile, final String baseFileName) throws IOException {
    final OneRowParquetSchema oneRowParquetSchema = new OneRowParquetSchema(inputFile);
    final Schema avroSchema = oneRowParquetSchema.extractAvroSchemaFromParquet();
    final String terminalID = oneRowParquetSchema.extractTerminalIDFromParquet();
    final String dirPath = FileUtils.getParentDir(inputFile);
    oneRowParquetSchema.writeOneRowParquetSchemaFile(terminalID, avroSchema, dirPath, baseFileName);
  }

  private Schema extractAvroSchemaFromParquet() throws IOException {
    try (final ParquetFileReader rdr = ParquetFileReader.open(nioPathToInputFile(inputFilePath))) {
      final org.apache.parquet.hadoop.metadata.FileMetaData prqFMD = rdr.getFooter().getFileMetaData();
      final String avroSchemaAsJsonText = prqFMD.getKeyValueMetaData().get(avroSchemaFieldName);
      return new Schema.Parser().setValidate(true).parse(avroSchemaAsJsonText);
    }
  }

  private String extractTerminalIDFromParquet() throws IOException {
    String term_id = "";
    try (final ParquetReader<GenericData.Record> reader = AvroParquetReader
          .<GenericData.Record>builder(nioPathToInputFile(inputFilePath))
          .withConf(new Configuration())
          .build())
    {
      GenericData.Record readRecord = reader.read();
      if (readRecord != null) {
        // input file contains data records so obtain a Terminal ID
        term_id = readRecord.get("TERM_ID$").toString();
      }
    }
    return term_id;
  }

  private void writeOneRowParquetSchemaFile(final String terminalID,
                                            final Schema avroSchema,
                                            final String dirPath,
                                            final String baseFileName)
        throws IOException
  {
    final Path schemaAsPrqPath = makeSchemaFilePathFromBaseFileName(inputFilePath, dirPath, baseFileName, parquetExtent);
    try (final ParquetWriter<GenericData.Record> prqWrt = makeParquetRecordWriter(avroSchema, schemaAsPrqPath)) {
      writeOneRowParquetFile(terminalID, avroSchema, prqWrt);
    }
  }

  private static ParquetWriter<GenericData.Record> makeParquetRecordWriter(final Schema avroSchema,
                                                                           final Path fileToWrite)
        throws IOException
  {
    final GenericData genericData = GenericData.get();
    genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());
    genericData.addLogicalTypeConversion(new TimeConversions.TimestampConversion());
    genericData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());
    genericData.addLogicalTypeConversion(new Conversions.UUIDConversion());
    return AvroParquetWriter
          .<GenericData.Record>builder(nioPathToOutputFile(fileToWrite))
          .withRowGroupSize(256 * 1024 * 1024)
          .withPageSize(128 * 1024)
          .withSchema(avroSchema)
          .withConf(new Configuration())
          .withCompressionCodec(CompressionCodecName.GZIP)
          .withValidation(false)
          .withDictionaryEncoding(false)
          .withDataModel(genericData)
          .build();
  }

  private static void writeOneRowParquetFile(final String terminalID,
                                             final Schema writerAvroSchema,
                                             final ParquetWriter<GenericData.Record> parquetRecordWriter)
        throws IOException
  {
    final GenericData.Record writeRecord = new GenericData.Record(writerAvroSchema);
    final List<Schema.Field> writeFields = writeRecord.getSchema().getFields();

    // reset the writeRecord to empty values initialized state
    for (final Schema.Field writeField : writeFields) {
      Object value = null;
      switch(writeField.name().toUpperCase()) {
        case "ID$": {
          value = 0L;
          break;
        }
        case "PRIMARY_KEY_VAL$": {
          value = new UUID( 0L , 0L ).toString();
          break;
        }
        case "TERM_ID$": {
          value = terminalID;
          break;
        }
        case "CREATED_DATE$": {
          value = Instant.EPOCH.toEpochMilli();
          break;
        }
        case "SOURCE_SCN$": {
          final MathContext mc = new MathContext(30, HALF_UP);
          value = new BigDecimal(0, mc).setScale(0, HALF_UP);
          break;
        }
        case "SQL_OPERATION$": {
          value = "INSERT";
          break;
        }
      }
      writeRecord.put(writeField.pos(), value);
    }

    // write record to destination output file
    parquetRecordWriter.write(writeRecord);
  }
}