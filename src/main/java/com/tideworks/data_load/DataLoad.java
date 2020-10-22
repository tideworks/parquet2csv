/* DataLoad.java
 *
 * Copyright June 2018-2020 Tideworks Technology
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
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import static com.tideworks.data_load.io.InputFile.nioPathToInputFile;
import static com.tideworks.data_load.io.OutputFile.makePositionOutputStream;
import static com.tideworks.data_load.util.io.FileUtils.jsonExtent;
import static com.tideworks.data_load.util.io.FileUtils.parquetExtent;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

@InvokeByteCodePatching
public class DataLoad {
  private static final String clsName = DataLoad.class.getSimpleName();
  private static final String eol = System.getProperty("line.separator");
  private static final File progDirPathFile;
          static final String logBackXml = "logback.xml";
  private static final String logsDirStr = "logs";
  private static final String abortPrgErrMsg = "cannot continue - aborting program:";
  private static final Supplier<Logger> clsLoggerFactory = () -> LoggerFactory.getLogger(clsName);
  private static Logger log;

  static { // initialization of static class fields
    progDirPathFile = insureLogBackXmlExist(FileSystems.getDefault().getPath(".").toFile());
    LoggingLevel.setLoggingVerbosity(LoggingLevel.INFO);
    log = LoggingLevel.effectLoggingLevel(clsLoggerFactory);
  }

  private static File insureLogBackXmlExist(final File progDirPathFile) {
    final File logsDir = new File(progDirPathFile, logsDirStr);
    String errmsg = abortPrgErrMsg;
    try {
      errmsg = String.format("logging directory could not be created: \"%s%c\" :", logsDir, File.separatorChar);
      Files.createDirectories(logsDir.toPath());
      final File logBackXmlFile = new File(progDirPathFile, logBackXml);
      errmsg = String.format("can't create: \"%s\" :", logBackXmlFile);
      if (logBackXmlFile.exists() && logBackXmlFile.isFile() && Files.size(logBackXmlFile.toPath()) > 0) {
        return progDirPathFile;
      }
      try (final InputStream logBackXmlRsrc = ClassLoader.getSystemResourceAsStream(logBackXml);
           final OutputStream logBackXmlOut = Files.newOutputStream(logBackXmlFile.toPath(), CREATE, TRUNCATE_EXISTING) )
      {
        assert logBackXmlRsrc != null;
        byte[] ioBuf = new byte[512];
        for (; ; ) {
          final int n = logBackXmlRsrc.read(ioBuf);
          if (n < 0) {
            break;
          } else if (n == 0) {
            continue;
          }
          logBackXmlOut.write(ioBuf, 0, n);
        }
        logBackXmlOut.flush();
      }
    } catch (IOException e) {
      System.err.println(errmsg);
      e.printStackTrace(System.err);
      System.exit(1);
    }
    return progDirPathFile;
  }

  static File getProgDirPath() { return progDirPathFile; }

  private static void usage() {
    final String msg = String.join(eol,
          "prq2csv usage:",
          "",
          "  prq2csv [options] file_path...",
          "",
          "  -?|-h|--help                     display this help information",
          "  -v|--verbosity level             level can be: trace, debug, info, warn, error (default: info)",
          "  -schema|--avro-schema file_path  specified Avro schema file is validated (but otherwise is not",
          "                                   utilized for processing)",
          "  -tz|--time-zone arg              time zone offset (in hours - plus or minus) or time zone ID name",
          "                                   (default is current timezone)",
          "  -to-json                         for each Parquet input file, its schema is exported to Avro json",
          "                                   format file - same base name while ending in .json",
          "  -from-json                       specified .json Avro schema file is converted to Parquet; output",
          "                                   file has same base name but now ending in .parquet",
          "  -one-row-schema                  from a specified Parquet file, generate a valid one row schema file",
          "                                   (populated by a dummy row, i.e., null columns)"
          );
    System.out.println(msg);
  }

  public static void main(String[] args) {
    if (args.length <= 0) {
      log.error("no Parquet input files were specified to be processed");
      usage();
      System.exit(1); // return non-zero status to indicate program failure
    }

    final Function<File, File> validateFile = filePath -> {
      if (!filePath.exists() || !filePath.isFile()) {
        log.error("does not exist or is not a valid file:{}\t\"{}\"", eol, filePath);
        System.exit(1); // return non-zero status to indicate program failure
      }
      return filePath;
    };

    try {
      final IntFunction<Optional<String>> getNextArg = index ->
                                                       index < args.length ? Optional.of(args[index]) : Optional.empty();

      Optional<File> schemaFileOptn = Optional.empty();
      Optional<ZoneId> timeZoneIdOptn = Optional.empty();
      final List<File> inputFiles = new ArrayList<>();
      boolean isExportSchemaToJson = false;
      boolean isImportJsonToSchema = false;
      boolean isMakeOneRowSchema = false;

      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        if (arg.charAt(0) == '-') {
          switch(arg.toLowerCase()) {
            case "-h":
            case "-?":
            case "--help": {
              usage();
              return;
            }
            case "-to-json": {
              isExportSchemaToJson = true;
              continue;
            }
            case "-from-json": {
              isImportJsonToSchema = true;
              continue;
            }
            case "-one-row-schema": {
              isMakeOneRowSchema = true;
              continue;
            }
          }
          final String[] argParts = arg.split("=", 2);
          final String option = argParts[0];
          switch (option.toLowerCase()) {
            case "-v":
            case "--verbosity": {
              final Supplier<Exception> missingVerbositySpecifier = () -> {
                final String errmsg = option + " => is missing logging verbosity specifier argument";
                return new Exception(errmsg);
              };
              arg = (argParts.length > 1 ? argParts[1] : getNextArg.apply(++i).orElseThrow(missingVerbositySpecifier)).trim();
              log = LoggingLevel.adjustLoggingLevel(arg, clsLoggerFactory);
              break;
            }
            case "-schema":
            case "--avro-schema": {
              final Supplier<Exception> missingAvroSchemaFilePath = () -> {
                final String errmsg = option + " => is missing Avro Schema file path specification argument";
                return new Exception(errmsg);
              };
              arg = (argParts.length > 1 ? argParts[1] : getNextArg.apply(++i).orElseThrow(missingAvroSchemaFilePath)).trim();
              schemaFileOptn = Optional.of(validateFile.apply(new File(arg)));
              break;
            }
            case "-tz":
            case "--time-zone": {
              final Supplier<Exception> missingTimeZoneSpecifier = () -> {
                final String errmsg = option + " => is missing time zone offset specifier argument";
                return new Exception(errmsg);
              };
              arg = (argParts.length > 1 ? argParts[1] : getNextArg.apply(++i).orElseThrow(missingTimeZoneSpecifier)).trim();
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
                final ZoneId zoneId = ZoneId.of(zoneOffset != null ? zoneOffset.getId() : arg);
                timeZoneIdOptn = Optional.of(zoneId.normalized());
              } catch (DateTimeException e) {
                log.error("invalid time zone offset:", e);
                System.exit(1); // return non-zero status to indicate program failure
              }
              break;
            }
            default: {
              log.warn("unknown command line option: '{}' - ignoring", arg);
            }
          }
        } else {
          // assume is a file path argument
          inputFiles.add(validateFile.apply(new File(arg)));
        }
      }

      if (schemaFileOptn.isPresent()) {
        final File avroSchemaFile = schemaFileOptn.get();
        ValidateAvroSchema.validate(avroSchemaFile);
        log.info("Avro schema file validated successfully{}\t\"{}\"", eol, avroSchemaFile);
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
      log.error(abortPrgErrMsg, e);
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