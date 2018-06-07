package com.tideworks.data_load;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public final class CustomClassLoader extends ClassLoader {
  private static final String failedLoadingErrMsgFmt = "failure custom loading class %s";
  private static final String classFileNotFoundErrMsgFmt = "unexpected error - %s did not exist or was not a file";
  private static final String classNotCustomLoadedWrnMsgFmt = "WARN: Class not found by custom loader:%n\t\"%s\"";
  private static final String uninitializedVariableErrMsg =
          "unexpected error - static variable avroSchemaClassesDir not initialized";
  private static File avroSchemaClassesDir;

  static void setAvroSchemaClassesDir(File avroSchemaClassesDir) {
    CustomClassLoader.avroSchemaClassesDir = avroSchemaClassesDir;
  }
  private static File getAvroSchemaClassesDir() {
    if (avroSchemaClassesDir == null) {
      throw new AssertionError(uninitializedVariableErrMsg);
    }
    return avroSchemaClassesDir;
  }

  public CustomClassLoader(ClassLoader parent) {
    super(parent);
    Thread.currentThread().setContextClassLoader(this);
    final String clsPath = System.getProperty("java.class.path");
    System.setProperty("java.class.path", "avro-classes" + File.pathSeparator + clsPath);
    System.err.printf("DEBUG: system loaded class: %s%n", this.getClass().getName());
  }

  static String getClassRelativeFilePath(final String pckgName, final String name) {
    final String dotClsStr = ".class";
    //noinspection StringBufferReplaceableByString
    return new StringBuilder(name.length() + dotClsStr.length())
            .append(pckgName.replace('.', File.separatorChar)).append(File.separatorChar)
            .append(name.substring(pckgName.length() + 1)).append(dotClsStr)
            .toString();
  }

  private static final String[] pckgPrefixes = { "org.apache.", "com.tideworks.", "org.slf4j.", "net.bytebuddy." };

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    boolean isPckgPrefixClass = false;
    for (final String pckgPrefix : pckgPrefixes) {
      if (name.startsWith(pckgPrefix)) {
        isPckgPrefixClass = true;
        break;
      }
    }
    if (isPckgPrefixClass) {
      System.err.printf("DEBUG: loading class: %s%n", name);
    }
    if (name.compareTo("org.apache.avro.Schema") == 0) {
      final String filterByPckgName = "org.apache.avro";
      final String relativeFilePath = getClassRelativeFilePath(filterByPckgName, name);
      final File clsFile = new File(getAvroSchemaClassesDir(), relativeFilePath);
      if (clsFile.exists() && clsFile.isFile()) {
        System.err.printf("DEBUG: looking to custom load class file:%n\t\"%s\"%n", clsFile);
        try {
          final byte[] clsBytes = Files.readAllBytes(clsFile.toPath());
          Class<?> rtnCls = this.defineClass(name, clsBytes, 0, clsBytes.length);
          this.resolveClass(rtnCls);
          if (rtnCls == null) {
            System.err.printf(classNotCustomLoadedWrnMsgFmt, name);
            rtnCls = super.loadClass(name);
          }
          return rtnCls;
        } catch(IOException e) {
          throw new ClassNotFoundException(String.format(failedLoadingErrMsgFmt, name), e);
        }
      } else {
        throw new AssertionError(String.format(classFileNotFoundErrMsgFmt, clsFile));
      }
    }
//    System.err.printf("DEBUG: loading class: %s%n", name);
    return super.loadClass(name);
  }

}