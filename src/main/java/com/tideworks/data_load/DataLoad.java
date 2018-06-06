package com.tideworks.data_load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class DataLoad {
  private static final Logger log;
  private static final String avroSchemaClassesSubDir = "avro-classes";
  private static final File progDirPathFile;

  static File getProgDirPath() { return progDirPathFile; }

  static {
    final Predicate<String> existsAndIsDir = dirPath -> {
      final File dirPathFile = new File(dirPath);
      return dirPathFile.exists() && dirPathFile.isDirectory();
    };
    String homeDirPath = System.getenv("HOME"); // user home directory
    homeDirPath = homeDirPath != null && !homeDirPath.isEmpty() && existsAndIsDir.test(homeDirPath) ? homeDirPath : ".";
    progDirPathFile = FileSystems.getDefault().getPath(homeDirPath).toFile();
    LoggingLevel.setLoggingVerbosity(LoggingLevel.DEBUG);
    log = LoggingLevel.effectLoggingLevel(() -> LoggerFactory.getLogger(DataLoad.class.getSimpleName()));

    try {
      ValidateSchema.redefineAvroSchemaClass(new File(progDirPathFile, avroSchemaClassesSubDir));
    } catch (ClassNotFoundException|IOException e) {
      uncheckedExceptionThrow(e);
    }
  }

  @SuppressWarnings({"unchecked", "UnusedReturnValue"})
  private static <T extends Throwable, R> R uncheckedExceptionThrow(Throwable t) throws T { throw (T) t; }

  public static void main(String[] args) {
    if (args.length <= 0) {
      log.error("no Parquet input files were specified to be processed");
      System.exit(1); // return non-zero status to indicate program failure
    }
    try {
      Optional<File> schemaFileOptn = Optional.empty();
      final List<File> inputFiles = new ArrayList<>();
      for(int i = 0; i < args.length; i++) {
        String arg = args[i];
        switch (arg.charAt(0)) {
          case '-': {
            final String option = arg.substring(1).toLowerCase();
            switch (option) {
              case "schema": {
                final int n = i + 1;
                if (n < args.length) {
                  arg = args[i = n];
                  final File filePath = new File(arg);
                  if (!filePath.exists() || !filePath.isFile()) {
                    log.error("\"{}\" does not exist or is not a valid file", arg);
                    System.exit(1); // return non-zero status to indicate program failure
                  }
                  schemaFileOptn = Optional.of(filePath);
                } else {
                  log.error("expected Arvo-schema file path after option {}", arg);
                  System.exit(1);
                }
                break;
              }
              default: {
                log.error("unknown command line option: {} - ignoring", arg);
              }
            }
            break;
          }
          default: {
            // assume is a file path argument
            final File filePath = new File(arg);
            if (!filePath.exists() || !filePath.isFile()) {
              log.error("\"{}\" does not exist or is not a valid file", arg);
              System.exit(1); // return non-zero status to indicate program failure
            }
            inputFiles.add(filePath);
          }
        }
      }

      if (!schemaFileOptn.isPresent()) {
        log.warn("no Arvo-schema file has been specified");
      }
      if (inputFiles.isEmpty() && !schemaFileOptn.isPresent()) {
        log.error("no Parquet input file have been specified for processing - cannot proceed");
        System.exit(1);
      }

      if (schemaFileOptn.isPresent()) {
        final File avroSchemaFile = schemaFileOptn.get();
        ValidateSchema.validate(avroSchemaFile);
        log.info("Avro schema file \"{}\" validated successfully", avroSchemaFile);
      }

      for(final File inputFile : inputFiles) {
        log.info("processing Parquet input file: \"{}\"", inputFile);
        ParquetToCsv.processToOutput(inputFile);
      }
    } catch (Throwable e) {
      log.error("program terminated due to exception:", e);
      System.exit(1); // return non-zero status to indicate program failure
    }
    log.info("program completion successful");
  }
}