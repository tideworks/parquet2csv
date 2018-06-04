package com.tideworks.data_load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class DataLoad {
  private static final Logger LOGGER;
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
    LOGGER = LoggingLevel.effectLoggingLevel(() -> LoggerFactory.getLogger(DataLoad.class.getSimpleName()));
  }

  public static void main(String[] args) {
    if (args.length <= 0) {
      LOGGER.error("no Parquet input files were specified to be processed");
      System.exit(1); // return non-zero status to indicate program failure
    }
    try {
      Optional<File> schemaFile = Optional.empty();
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
                    LOGGER.error("\"{}\" does not exist or is not a valid file", arg);
                    System.exit(1); // return non-zero status to indicate program failure
                  }
                  schemaFile = Optional.of(filePath);
                } else {
                  LOGGER.error("expected Arvo-schema file path after option {}", arg);
                  System.exit(1);
                }
                break;
              }
              default: {
                LOGGER.error("unknown command line option: {} - ignoring", arg);
              }
            }
            break;
          }
          default: {
            // assume is a file path argument
            final File filePath = new File(arg);
            if (!filePath.exists() || !filePath.isFile()) {
              LOGGER.error("\"{}\" does not exist or is not a valid file", arg);
              System.exit(1); // return non-zero status to indicate program failure
            }
            inputFiles.add(filePath);
          }
        }
      }

      if (!schemaFile.isPresent()) {
        LOGGER.error("no Arvo-schema file has been specified - cannot proceed");
        System.exit(1);
      }
      if (inputFiles.isEmpty()) {
        LOGGER.error("no Parquet input file have been specified for processing - cannot proceed");
        System.exit(1);
      }

    } catch (Throwable e) {
      LOGGER.error("program terminated due to exception:", e);
      System.exit(1); // return non-zero status to indicate program failure
    }
    LOGGER.info("program completion successful");
  }
}