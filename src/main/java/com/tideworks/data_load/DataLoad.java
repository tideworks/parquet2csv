/* DataLoad.java
 *
 * Copyright June 2018 Tideworks Technology
 * Author: Roger D. Voss
 * MIT License
 */
package com.tideworks.data_load;

import com.tideworks.annotation.InvokeByteCodePatching;
import com.tideworks.data_load.util.JsonAvroSchemaSerializer;
import com.tideworks.data_load.util.JsonStrMapSerializer;
import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.tideworks.data_load.io.InputFile.nioPathToInputFile;
import static com.tideworks.data_load.io.OutputFile.makePositionOutputStream;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

@InvokeByteCodePatching
public class DataLoad {
  private static final String clsName = DataLoad.class.getSimpleName();
  private static final Logger log;
  private static final File progDirPathFile;
  private static final String parquetExtent = ".parquet";
  private static final String jsonExtent = ".json";

  static {
    final Predicate<String> existsAndIsDir = dirPath -> {
      final File dirPathFile = new File(dirPath);
      return dirPathFile.exists() && dirPathFile.isDirectory();
    };
    String homeDirPath = System.getenv("HOME"); // user home directory
    homeDirPath = homeDirPath != null && !homeDirPath.isEmpty() && existsAndIsDir.test(homeDirPath) ? homeDirPath : ".";
    progDirPathFile = FileSystems.getDefault().getPath(homeDirPath).toFile();
    LoggingLevel.setLoggingVerbosity(LoggingLevel.DEBUG);
    log = LoggingLevel.effectLoggingLevel(() -> LoggerFactory.getLogger(clsName));
  }

  static File getProgDirPath() { return progDirPathFile; }

  public static void main(String[] args) {
    if (args.length <= 0) {
      log.error("no Parquet input files were specified to be processed");
      System.exit(1); // return non-zero status to indicate program failure
    }

    final Consumer<File> validateFile = filePath -> {
      if (!filePath.exists() || !filePath.isFile()) {
        log.error("\"{}\" does not exist or is not a valid file", filePath);
        System.exit(1); // return non-zero status to indicate program failure
      }
    };

    try {
      Optional<File> schemaFileOptn = Optional.empty();
      Optional<String> timeZoneOffsetOptn = Optional.empty();
      final List<File> inputFiles = new ArrayList<>();
      boolean exportSchemaToJson = false;
      boolean importJsonToSchema = false;

      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        if (arg.charAt(0) == '-') {
          final String[] argParts = arg.split("=", 2);
          final String option = argParts[0].substring(1).toLowerCase();
          switch (option) {
            case "schema": {
              if (argParts.length > 1) {
                arg = argParts[1];
              } else {
                final int n = i + 1;
                if (n < args.length) {
                  arg = args[i = n];
                } else {
                  log.warn("expected Arvo-schema file path after option {}", argParts[0]);
                  break;
                }
              }
              final File filePath = new File(arg);
              validateFile.accept(filePath);
              schemaFileOptn = Optional.of(filePath);
              break;
            }
            case "tz-offset": {
              if (argParts.length > 1) {
                arg = argParts[1];
              } else {
                final int n = i + 1;
                if (n < args.length) {
                  arg = args[i = n];
                } else {
                  log.warn("expected time zone offset specifier after option {}", argParts[0]);
                  break;
                }
              }
              timeZoneOffsetOptn = Optional.of(arg);
              break;
            }
            case "to-json": {
              exportSchemaToJson = true;
              break;
            }
            case "from-json": {
              importJsonToSchema = true;
              break;
            }
            default: {
              log.warn("unknown command line option: '{}' - ignoring", arg);
            }
          }
        } else {
          // assume is a file path argument
          final File filePath = new File(arg);
          validateFile.accept(filePath);
          inputFiles.add(filePath);
        }
      }

      if (schemaFileOptn.isPresent()) {
        final File avroSchemaFile = schemaFileOptn.get();
        ValidateAvroSchema.validate(avroSchemaFile);
        log.info("Avro schema file \"{}\" validated successfully", avroSchemaFile);
      }

      if (!inputFiles.isEmpty()) {
        final ZoneId timeZoneId = timeZoneOffsetOptn.isPresent()
                ? ZoneOffset.of(timeZoneOffsetOptn.get()).normalized() : ZoneId.systemDefault();

        for(final File inputFile : inputFiles) {
          final String fileNameLC = inputFile.getName().toLowerCase();
          final String ext = fileNameLC.endsWith(".parquet") ? "Parquet " : fileNameLC.endsWith(".json") ? "JSON " : "";
          log.info("processing {}input file: \"{}\"", ext, inputFile);

          boolean isParquet = false, isJson = false;

          String baseFileName = inputFile.getName(), baseFileNameLC = baseFileName.toLowerCase();
          int index = baseFileNameLC.endsWith(parquetExtent) ? baseFileNameLC.lastIndexOf(parquetExtent) : -1;
          if (index != -1) {
            baseFileName = baseFileName.substring(0, index);
            isParquet = true;
          } else {
            index = baseFileNameLC.endsWith(jsonExtent) ? baseFileNameLC.lastIndexOf(jsonExtent) : -1;
            if (index != -1) {
              baseFileName = baseFileName.substring(0, index);
              isJson = true;
            }
          }

          if (exportSchemaToJson && isParquet) {
            // extract schema from .parquet file and write into a companion .json file
            String schemaAsJson;
            try (final ParquetFileReader rdr = ParquetFileReader.open(nioPathToInputFile(inputFile.toPath()))) {
              schemaAsJson = MyParquetMetadata.toPrettyJSON(rdr.getFooter());
            }
            final String dirPath = ParquetToCsv.getParentDir(inputFile);
            final String fileName = baseFileName + jsonExtent;
            final Path schemaAsJsonFilePath = Paths.get(dirPath, fileName);
            try (final Writer writer = Files.newBufferedWriter(schemaAsJsonFilePath, CREATE, TRUNCATE_EXISTING)) {
              writer.write(schemaAsJson);
            }
            if (Files.size(schemaAsJsonFilePath) <= 0) {
              Files.delete(schemaAsJsonFilePath);
              log.warn("schema-as-JSON file was empty (and was deleted): \"{}\"", schemaAsJsonFilePath);
            }
          } else if (importJsonToSchema && isJson) {
            // load schema from .json file and write into a .parquet file
            final Path jsonSchemaFilePath = inputFile.toPath();
            if (Files.exists(jsonSchemaFilePath) && Files.isRegularFile(jsonSchemaFilePath)
                && Files.size(jsonSchemaFilePath) > 0)
            {
              ParquetMetadata parquetMetadata;
              try (final Reader jsonReader = Files.newBufferedReader(jsonSchemaFilePath)) {
                parquetMetadata = MyParquetMetadata.fromJSON(jsonReader);
              }
              final String dirPath = ParquetToCsv.getParentDir(inputFile);
              final String fileName = baseFileName + parquetExtent;
              final Path schemaAsParquetFilePath = Paths.get(dirPath, fileName);
              final OutputStream outStream = Files.newOutputStream(schemaAsParquetFilePath, CREATE, TRUNCATE_EXISTING);
              try (final PositionOutputStream out = makePositionOutputStream(() -> outStream)) {
                serializeFullFooter(parquetMetadata, out);
              }
            } else {
              log.warn("JSON schema file empty (or invalid) - skipping: \"{}\"", jsonSchemaFilePath);
            }
            continue;
          }

          // write a .parquet file to pseudo .csv
          ParquetToCsv.processToOutput(timeZoneId, inputFile);
        }

      } else if (!schemaFileOptn.isPresent()) {
        log.error("no Parquet input file have been specified for processing - cannot proceed");
        System.exit(1);
      }
    } catch (Throwable e) {
      log.error("program terminated due to exception:", e);
      System.exit(1); // return non-zero status to indicate program failure
    }
    log.info("program completion successful");
  }

  private static void serializeFullFooter(final ParquetMetadata footer, final PositionOutputStream out)
        throws IOException
  {
    out.write(ParquetFileWriter.MAGIC);
    serializeFooter(footer, out, (fileMetadata, to, writeOut) -> writeOut.write(fileMetadata, to));
  }

  @FunctionalInterface
  private interface WriteFileMetaData {
    void write(org.apache.parquet.format.FileMetaData fileMetadata, OutputStream to) throws java.io.IOException;
  }

  @FunctionalInterface
  private interface CustomizeWriteFileMetaData {
    void customize(org.apache.parquet.format.FileMetaData fileMetadata, OutputStream to, WriteFileMetaData writeOut)
          throws java.io.IOException;
  }

  //
  // Based on:
  //  org.apache.parquet.hadoop.ParquetFileWriter.serializeFooter(ParquetMetadata footer, PositionOutputStream out)
  //    throws IOException;
  //
  // From:
  //  parquet-hadoop-1.10.0-sources.jar
  //
  private static void serializeFooter(final ParquetMetadata footer,
                                      final PositionOutputStream out,
                                      final CustomizeWriteFileMetaData writeCustomizer)
        throws IOException
  {
    final long footerIndex = out.getPos();
    final ParquetMetadataConverter metadataCnvtr = new ParquetMetadataConverter();
    final org.apache.parquet.format.FileMetaData parquetFileMetadata =
          metadataCnvtr.toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
    writeCustomizer.customize(parquetFileMetadata, out, Util::writeFileMetaData);
    log.debug("{}: footer length = {}", out.getPos(), (out.getPos() - footerIndex));
    BytesUtils.writeIntLittleEndian(out, (int) (out.getPos() - footerIndex));
    out.write(ParquetFileWriter.MAGIC);
  }

  // class that is an analog to the Parquet library FileMetaData class
  // but the schema field is of type Avro Schema instead of MessageType
  @SuppressWarnings("unused")
  public static final class FileMetaData implements Serializable {
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

    @JsonSerialize(using=JsonAvroSchemaSerializer.AvroSchemaSerializer.class)
    public Schema getSchema() {
      return schema;
    }
    @JsonDeserialize(using=JsonAvroSchemaSerializer.AvroSchemaDeserializer.class)
    public void setSchema(Schema schema) { this.schema = schema; }

    @JsonSerialize(using=JsonStrMapSerializer.StringMapSerializer.class)
    public Map<String, String> getKeyValueMetaData() { return keyValueMetaData; }
    @JsonDeserialize(using=JsonStrMapSerializer.StringMapDeserializer.class)
    public void setKeyValueMetaData(Map<String, String> keyValueMetaData) { this.keyValueMetaData = keyValueMetaData; }

    public String getCreatedBy() {
      return createdBy;
    }
    public void setCreatedBy(String createdBy) {
      this.createdBy = createdBy;
    }
  }

  @SuppressWarnings("WeakerAccess")
  public static final class MyParquetMetadata {

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
  }
}