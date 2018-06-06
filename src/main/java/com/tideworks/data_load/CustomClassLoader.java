package com.tideworks.data_load;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static com.tideworks.data_load.DataLoad.getAvroSchemaClassesDir;

public final class CustomClassLoader extends ClassLoader {
  private static final String failedLoadingErrMsgFmt = "failure custom loading class %s";
  private static final String classFileNotFoundErrMsgFmt = "unexpected error - %s did not exist or was not a file";
  private static final String classNotCustomLoadedWrnMsgFmt = "WARN: Class not found by custom loader:%n\t\"%s\"";

  public CustomClassLoader(ClassLoader parent) {
    super(parent);
    Thread.currentThread().setContextClassLoader(this);
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    if (name.startsWith("org.apache.")) {
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

  static String getClassRelativeFilePath(String filterByPckgName, String name) {
    final String dotClsStr = ".class";
    final StringBuilder sb = new StringBuilder(name.length() + dotClsStr.length())
            .append(filterByPckgName.replace('.', File.separatorChar)).append(File.separatorChar)
            .append(name.substring(filterByPckgName.length() + 1)).append(dotClsStr);
    return sb.toString();
  }
}