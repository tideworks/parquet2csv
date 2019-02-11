/* DataLoad.java
 *
 * Copyright June 2018 Tideworks Technology
 * Author: Roger D. Voss
 * MIT License
 */
package com.tideworks.data_load;

import com.tideworks.annotation.InvokeByteCodePatching;
import com.tideworks.data_load.util.io.ParquetMetadataToBinarySerialize;
import com.tideworks.data_load.util.io.ParquetMetadataToJsonSerialize;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.PositionOutputStream;
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
  private static final String schemaExtent = ".schema";

  static { // initialization of static class fields
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
      boolean isExportSchemaToJson = false;
      boolean isImportJsonToSchema = false;

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
              isExportSchemaToJson = true;
              break;
            }
            case "from-json": {
              isImportJsonToSchema = true;
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
          final String fileTypeDesc = fileNameLC.endsWith(parquetExtent)
                ? "Parquet " : (fileNameLC.endsWith(jsonExtent) ? "JSON " : "");

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

          if (isParquet || isJson) {
            log.info("processing {}input file: \"{}\"", fileTypeDesc, inputFile);
          }

          if (isExportSchemaToJson && isParquet) {
            // extract schema from .parquet file and write into a companion .json file
            extractParquetMetadataToJson(inputFile, baseFileName);
          } else if (isImportJsonToSchema && isJson) {
            // load schema from .json file and write into a .parquet file
            loadParquetMetadataFromJson(inputFile, baseFileName);
            continue;
          }

          if (isParquet) {
            // write a .parquet file to pseudo .csv
            ParquetToCsv.processToOutput(timeZoneId, inputFile);
          } else {
            log.error("not a recognized file type for processing: \"{}\"", inputFile);
          }
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

  private static void extractParquetMetadataToJson(File inputFile, String baseFileName) throws IOException {
    String schemaAsJson;
    try (final ParquetFileReader rdr = ParquetFileReader.open(nioPathToInputFile(inputFile.toPath()))) {
      schemaAsJson = ParquetMetadataToJsonSerialize.toPrettyJSON(rdr.getFooter());
    }
    final String dirPath = ParquetToCsv.getParentDir(inputFile);
    final String fileName = baseFileName + schemaExtent + jsonExtent;
    final Path schemaAsJsonFilePath = Paths.get(dirPath, fileName);
    try (final Writer writer = Files.newBufferedWriter(schemaAsJsonFilePath, CREATE, TRUNCATE_EXISTING)) {
      writer.write(schemaAsJson);
    }
    if (Files.size(schemaAsJsonFilePath) <= 0) {
      Files.delete(schemaAsJsonFilePath);
      log.warn("schema-as-JSON file was empty (and was deleted): \"{}\"", schemaAsJsonFilePath);
    }
  }

  private static void loadParquetMetadataFromJson(File inputFile, String baseFileName) throws IOException {
    final Path jsonSchemaFilePath = inputFile.toPath();
    if (Files.exists(jsonSchemaFilePath) && Files.isRegularFile(jsonSchemaFilePath)
          && Files.size(jsonSchemaFilePath) > 0)
    {
      ParquetMetadata parquetMetadata;
      try (final Reader jsonReader = Files.newBufferedReader(jsonSchemaFilePath)) {
        parquetMetadata = ParquetMetadataToJsonSerialize.fromJSON(jsonReader);
      }
      final String dirPath = ParquetToCsv.getParentDir(inputFile);
      final String fileName = baseFileName.endsWith(schemaExtent)
            ? baseFileName + parquetExtent : baseFileName + schemaExtent + parquetExtent;
      final Path schemaAsParquetFilePath = Paths.get(dirPath, fileName);
      final OutputStream outStream = Files.newOutputStream(schemaAsParquetFilePath, CREATE, TRUNCATE_EXISTING);
      try (final PositionOutputStream out = makePositionOutputStream(() -> outStream)) {
        ParquetMetadataToBinarySerialize.serializeFullFooter(parquetMetadata, out);
      }
    } else {
      log.warn("JSON schema file empty (or invalid) - skipping: \"{}\"", jsonSchemaFilePath);
    }
  }
}