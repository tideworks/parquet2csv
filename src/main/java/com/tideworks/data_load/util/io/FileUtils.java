package com.tideworks.data_load.util.io;

import java.io.File;
import java.nio.file.Path;

@SuppressWarnings("WeakerAccess")
public class FileUtils {
  public static final String schemaExtent = ".schema";
  public static final String parquetExtent = ".parquet";
  public static final String jsonExtent = ".json";

  public static String getParentDir(File file) {
    String parentDir = file.getParent();
    if (parentDir == null || parentDir.isEmpty()) {
      parentDir = ".";
    }
    return parentDir;
  }

  public static Path makeSchemaFilePathFromBaseFileName(File inputFile, String baseFileName, String extent) {
    return makeSchemaFilePathFromBaseFileName(inputFile.toPath(), getParentDir(inputFile), baseFileName, extent);
  }

  public static Path makeSchemaFilePathFromBaseFileName(Path inputFile, String baseFileName, String extent) {
    return makeSchemaFilePathFromBaseFileName(inputFile, getParentDir(inputFile.toFile()), baseFileName, extent);
  }

  public static Path makeSchemaFilePathFromBaseFileName(Path inputFile, String dirPath, String baseFileName,
                                                        String extent)
  {
    final String fileName = baseFileName + (baseFileName.endsWith(schemaExtent) ? extent : schemaExtent + extent);
    return inputFile.getFileSystem().getPath(dirPath, fileName);
  }
}