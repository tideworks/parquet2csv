/* DataLoad.java
 *
 * Copyright June 2018 Tideworks Technology
 * Author: Roger D. Voss
 * MIT License
 */
package com.tideworks.data_load;

import com.tideworks.annotation.InvokeByteCodePatching;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.tideworks.data_load.io.InputFile.nioPathToInputFile;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

@InvokeByteCodePatching
public class DataLoad {
  private static final String clsName = DataLoad.class.getSimpleName();
  private static final Logger log;
  private static final File progDirPathFile;

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
          log.info("processing Parquet input file: \"{}\"", inputFile);

          if (exportSchemaToJson) {
            String baseFileName = inputFile.getName(), baseFileNameLC = baseFileName.toLowerCase();
            final String parquetExtent = ".parquet";
            final int index = baseFileNameLC.endsWith(parquetExtent) ? baseFileNameLC.lastIndexOf(parquetExtent) : -1;
            if (index != -1) {
              baseFileName = baseFileName.substring(0, index);
            }
            baseFileName += ".json";
            String schemaAsJson;
            try (final ParquetFileReader rdr = ParquetFileReader.open(nioPathToInputFile(inputFile.toPath()))) {
              schemaAsJson = ParquetMetadata.toPrettyJSON(rdr.getFooter());
            }
            final Path schemaAsJsonFilePath = Paths.get(inputFile.getParent(), baseFileName);
            try (final Writer writer = Files.newBufferedWriter(schemaAsJsonFilePath, CREATE, TRUNCATE_EXISTING)) {
              writer.write(schemaAsJson);
            }
            if (Files.size(schemaAsJsonFilePath) <= 0) {
              Files.delete(schemaAsJsonFilePath);
              log.warn("schema-as-JSON file was empty (and was deleted): \"{}\"", schemaAsJsonFilePath);
            }
          }

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
}