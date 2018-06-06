package com.tideworks.data_load;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.pool.TypePool;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.tideworks.data_load.io.InputFile.nioPathToInputFile;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static net.bytebuddy.matcher.ElementMatchers.*;
import static net.bytebuddy.matcher.ElementMatchers.isStatic;

public class DataLoad {
  private static final Logger LOGGER;
  private static final File progDirPathFile;
  private static final String csvDelimiter = ",";
  private static final String fileExtent = ".parquet";
  @SuppressWarnings("unused")
  private static final ClassLoader avroSchemaClassLoader;

  static File getProgDirPath() { return progDirPathFile; }

  static {
    final Predicate<String> existsAndIsDir = dirPath -> {
      final File dirPathFile = new File(dirPath);
      return dirPathFile.exists() && dirPathFile.isDirectory();
    };
    ClassLoader clsLdr = null;
    try {
      clsLdr = redefineAvroSchemaClass();
    } catch (ClassNotFoundException|IOException e) {
      uncheckedExceptionThrow(e);
    }
    avroSchemaClassLoader = clsLdr;
    String homeDirPath = System.getenv("HOME"); // user home directory
    homeDirPath = homeDirPath != null && !homeDirPath.isEmpty() && existsAndIsDir.test(homeDirPath) ? homeDirPath : ".";
    progDirPathFile = FileSystems.getDefault().getPath(homeDirPath).toFile();
    LoggingLevel.setLoggingVerbosity(LoggingLevel.DEBUG);
    LOGGER = LoggingLevel.effectLoggingLevel(() -> LoggerFactory.getLogger(DataLoad.class.getSimpleName()));
  }

  public static final class AvroSchemaInterceptor {
    @SuppressWarnings("unused")
    public static String validateName(String name) {
      System.out.println("intercept validateName() called");
      System.exit(1);
      return name;
    }
  }

  private static ClassLoader redefineAvroSchemaClass() throws ClassNotFoundException, IOException {
    final CustomClassLoader sysClassLoader = (CustomClassLoader) ClassLoader.getSystemClassLoader();
    final Class<?>[] methArgTypesMatch = { String.class };
    final TypePool pool = TypePool.Default.ofClassPath();
    final DynamicType.Unloaded<?> avroSchemaClsUnloaded = new ByteBuddy()
            .rebase(pool.describe("org.apache.avro.Schema").resolve(), ClassFileLocator.ForClassLoader.ofClassPath())
            .method(named("validateName").and(returns(String.class)).and(takesArguments(methArgTypesMatch))
                    .and(isPrivate()).and(isStatic()))
            .intercept(MethodDelegation.to(AvroSchemaInterceptor.class))
            .make();
    final DynamicType.Loaded<?> loaded = avroSchemaClsUnloaded.load(sysClassLoader,
                                                                    ClassLoadingStrategy.Default.CHILD_FIRST);
    loaded.saveIn(new File("AvroSchemaClass.class"));
    final Class<?> avroSchemaCls = loaded.getLoaded();
    final ClassLoader avroSchemaClassLoader = avroSchemaCls.getClassLoader();
    sysClassLoader.setDelegatingClassLoader(avroSchemaClassLoader);
    sysClassLoader.cls = avroSchemaCls;
    Thread.currentThread().setContextClassLoader(sysClassLoader);
    assert ClassLoader.getSystemClassLoader() == sysClassLoader;
    assert ClassLoader.getSystemClassLoader().loadClass("org.apache.avro.Schema") == avroSchemaCls;
    return avroSchemaClassLoader;
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

      final Schema arvoSchema = new Schema.Parser().setValidate(true).parse(schemaFile.get());
      final List<String> fieldNames = arvoSchema.getFields().stream()
              .map(field -> field.name().toUpperCase())
              .collect(Collectors.toList());
      final String hdrRow = String.join(csvDelimiter, fieldNames.toArray(new String[0]));
      System.out.println(hdrRow);

      for(final File inputFile : inputFiles) {
        readParquetInputFile(inputFile);
      }
    } catch (Throwable e) {
      LOGGER.error("program terminated due to exception:", e);
      System.exit(1); // return non-zero status to indicate program failure
    }
    LOGGER.info("program completion successful");
  }

  private static void readParquetInputFile(final File inputFile) throws IOException {
    final String fileName = inputFile.getName();
    if (!fileName.endsWith(fileExtent)) {
      LOGGER.error("\"{}\" does not end in '{}' - thus is not assumed to be a Parquet file and is being skipped",
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
    final Writer outputWriter = new BufferedWriter(new OutputStreamWriter(csvOutputStream, utf8), ioStreamBufSize);

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
  private static <T extends Throwable, R> R uncheckedExceptionThrow(Throwable t) throws T { throw (T) t; }

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