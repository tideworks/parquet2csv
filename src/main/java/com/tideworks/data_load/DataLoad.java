package com.tideworks.data_load;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.pool.TypePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.tideworks.data_load.CustomClassLoader.getClassRelativeFilePath;
import static net.bytebuddy.matcher.ElementMatchers.*;
import static net.bytebuddy.matcher.ElementMatchers.isStatic;

public class DataLoad {
  private static final Logger log;
  private static final File progDirPathFile;
  private static final String customClsLoaderNotSetErrMsgFmt =
          "need to specify the custom class loader to the JVM:%n\t-Djava.system.class.loader=%s";
  private static final String avroSchemaClassesSubDir = "avro-classes";
  private static final File avroSchemaClassesDir;

  static File getProgDirPath() { return progDirPathFile; }
  static File getAvroSchemaClassesDir() { return avroSchemaClassesDir; }

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

    avroSchemaClassesDir = new File(progDirPathFile, avroSchemaClassesSubDir);
    try {
      redefineAvroSchemaClass(avroSchemaClassesDir);
    } catch (ClassNotFoundException|IOException e) {
      ParquetToCsv.uncheckedExceptionThrow(e);
    }
  }

  public static final class AvroSchemaInterceptor {
    @SuppressWarnings("unused")
    public static String validateName(String name) {
      System.out.println("intercept validateName() called");
      return name;
    }
  }

  private static void redefineAvroSchemaClass(File avroSchemaClassesDirPrm) throws ClassNotFoundException, IOException {
    final String avroSchemaClassPckgName = "org.apache.avro";
    final String avroSchemaClassName = avroSchemaClassPckgName + ".Schema";
    final String relativeFilePath = getClassRelativeFilePath(avroSchemaClassPckgName, avroSchemaClassName);
    final File clsFile = new File(avroSchemaClassesDirPrm, relativeFilePath);
    if (clsFile.exists() && clsFile.isFile()) {
      System.err.printf("DEBUG: class file already exist:%n\t\"%s\"%n", clsFile);
      return;
    }

    final ClassLoader sysClassLoader = ClassLoader.getSystemClassLoader();
    if (!(sysClassLoader instanceof CustomClassLoader)) {
      throw new AssertionError(String.format(customClsLoaderNotSetErrMsgFmt, ClassLoader.class.getName()));
    }
    final Class<?>[] methArgTypesMatch = { String.class };
    final TypePool pool = TypePool.Default.ofClassPath();
    final DynamicType.Unloaded<?> avroSchemaClsUnloaded = new ByteBuddy()
            .rebase(pool.describe(avroSchemaClassName).resolve(), ClassFileLocator.ForClassLoader.ofClassPath())
            .method(named("validateName").and(returns(String.class)).and(takesArguments(methArgTypesMatch))
                    .and(isPrivate()).and(isStatic()))
            .intercept(MethodDelegation.to(AvroSchemaInterceptor.class))
            .make();
    avroSchemaClsUnloaded.saveIn(avroSchemaClassesDirPrm);
  }

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
        log.error("no Arvo-schema file has been specified - cannot proceed");
        System.exit(1);
      }
      if (inputFiles.isEmpty()) {
        log.error("no Parquet input file have been specified for processing - cannot proceed");
        System.exit(1);
      }

      ValidateSchema.validate(schemaFileOptn.get());

      for(final File inputFile : inputFiles) {
        ParquetToCsv.processToOutput(inputFile);
      }
    } catch (Throwable e) {
      log.error("program terminated due to exception:", e);
      System.exit(1); // return non-zero status to indicate program failure
    }
    log.info("program completion successful");
  }
}