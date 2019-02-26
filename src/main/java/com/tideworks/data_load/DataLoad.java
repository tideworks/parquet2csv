/* DataLoad.java
 *
 * Copyright June 2018 Tideworks Technology
 * Author: Roger D. Voss
 * MIT License
 */
package com.tideworks.data_load;

import com.tideworks.annotation.InvokeByteCodePatching;
import com.tideworks.data_load.util.io.FileUtils;
import com.tideworks.data_load.util.io.OneRowParquetSchema;
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
import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.tideworks.data_load.io.InputFile.nioPathToInputFile;
import static com.tideworks.data_load.io.OutputFile.makePositionOutputStream;
import static com.tideworks.data_load.util.io.FileUtils.jsonExtent;
import static com.tideworks.data_load.util.io.FileUtils.parquetExtent;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

@InvokeByteCodePatching
public class DataLoad {
  private static final String clsName = DataLoad.class.getSimpleName();
  private static final Logger log;
  private static final File progDirPathFile;

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

    final Runnable usage = () -> {
      final String msg = String.join("\n",
            "prq2csv usage:\n",
            "\tprq2csv [options] file_path...\n",
            "\t-?|-h              display this help information",
            "\t-schema file_path  specified Avro schema file is validated (but",
            "\t                   otherwise is not utilized for processing)",
            "\t-tz arg            time zone offset (in hours - plus or minus) or",
            "\t                   time zone ID name (default is current timezone)",
            "\t-to-json           per each Parquet input file, its schema is exported to",
            "\t                   Avro json format file - same base name ending in .json",
            "\t-from-json         specified .json Avro schema file is converted to Parquet;",
            "\t                   output file has same base name but now ending in .parquet",
            "\t-one-row-schema    from a specified Parquet file, generate a valid one row",
            "\t                   schema file (populated by a dummy row, i.e., null columns)\n"
      );
      System.out.print(msg);
      System.exit(0);
    };

    final Consumer<File> validateFile = filePath -> {
      if (!filePath.exists() || !filePath.isFile()) {
        log.error("\"{}\" does not exist or is not a valid file", filePath);
        System.exit(1); // return non-zero status to indicate program failure
      }
    };

    try {
      Optional<File> schemaFileOptn = Optional.empty();
      Optional<ZoneId> timeZoneIdOptn = Optional.empty();
      final List<File> inputFiles = new ArrayList<>();
      boolean isExportSchemaToJson = false;
      boolean isImportJsonToSchema = false;
      boolean isMakeOneRowSchema = false;

      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        if (arg.charAt(0) == '-') {
          if (arg.length() == 2 && (arg.charAt(1) == '?' || arg.charAt(1) == 'h')) {
            usage.run();
          }
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
            case "tz": {
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
              ZoneOffset zoneOffset = null;
              try {
                final int hoursOffset = Integer.parseInt(arg);
                zoneOffset = ZoneOffset.ofHours(hoursOffset);
              } catch (NumberFormatException ignore) {
              } catch (DateTimeException e) {
                log.error("invalid time zone offset:", e);
                System.exit(1); // return non-zero status to indicate program failure
              }
              try {
                timeZoneIdOptn = Optional.of(ZoneId.of(zoneOffset != null ? zoneOffset.getId() : arg).normalized());
              } catch (NumberFormatException | DateTimeException e) {
                log.error("invalid time zone offset:", e);
                System.exit(1); // return non-zero status to indicate program failure
              }
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
            case "one-row-schema": {
              isMakeOneRowSchema = true;
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
        final ZoneId timeZoneId = timeZoneIdOptn.orElse(ZoneId.systemDefault());

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

          if (isParquet) {
            if (isExportSchemaToJson) {
              // extract schema from .parquet file and write into a companion .json file
              extractParquetMetadataToJson(inputFile, baseFileName);
              continue;
            }
            if (isMakeOneRowSchema) {
              OneRowParquetSchema.writeSchemaFile(inputFile, baseFileName);
              continue;
            }
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
    final Path schemaAsJsonPath = FileUtils.makeSchemaFilePathFromBaseFileName(inputFile, baseFileName, jsonExtent);
    try (final Writer writer = Files.newBufferedWriter(schemaAsJsonPath, CREATE, TRUNCATE_EXISTING)) {
      writer.write(schemaAsJson);
    }
    if (Files.size(schemaAsJsonPath) <= 0) {
      Files.delete(schemaAsJsonPath);
      log.warn("schema-as-JSON file was empty (and was deleted): \"{}\"", schemaAsJsonPath);
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
      final Path schemaAsPrqPath = FileUtils.makeSchemaFilePathFromBaseFileName(inputFile, baseFileName, parquetExtent);
      final OutputStream outStream = Files.newOutputStream(schemaAsPrqPath, CREATE, TRUNCATE_EXISTING);
      try (final PositionOutputStream out = makePositionOutputStream(() -> outStream)) {
        ParquetMetadataToBinarySerialize.serializeFullFooter(parquetMetadata, out);
      }
    } else {
      log.warn("JSON schema file empty (or invalid) - skipping: \"{}\"", jsonSchemaFilePath);
    }
  }
}