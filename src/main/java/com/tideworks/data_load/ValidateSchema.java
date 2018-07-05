/* ValidateSchema.java
 *
 * Copyright June 2018 Tideworks Technology
 * Author: Roger D. Voss
 * MIT License
 */
package com.tideworks.data_load;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import static net.bytebuddy.matcher.ElementMatchers.*;

class ValidateSchema {
  private static final Logger log = LoggerFactory.getLogger(ValidateSchema.class.getSimpleName());
  private static final String csvDelimiter = ",";

  static void validate(final File schemaFile) throws IOException {
    final Schema arvoSchema = new Schema.Parser().setValidate(true).parse(schemaFile);
    final List<String> fieldNames = arvoSchema.getFields().stream()
            .map(field -> field.name().toUpperCase())
            .collect(Collectors.toList());
    if (log.isInfoEnabled()) {
      log.info(String.join(csvDelimiter, fieldNames.toArray(new String[0])));
    }
  }

  public static final class AvroSchemaInterceptor {
    @SuppressWarnings("unused")
    public static String validateName(String name) {
      log.debug("intercept validateName() called");
      return name;
    }
  }

  static void redefineAvroSchemaClass(File avroSchemaClassesDirPrm) throws ClassNotFoundException, IOException {
    final String avroSchemaClassPckgName = "org.apache.avro";
    final String avroSchemaClassName = avroSchemaClassPckgName + ".Schema";
    final String relativeFilePath = getClassRelativeFilePath(avroSchemaClassPckgName, avroSchemaClassName);
    final File clsFile = new File(avroSchemaClassesDirPrm, relativeFilePath);
    Files.deleteIfExists(clsFile.toPath());
    final Class<?>[] methArgTypesMatch = { String.class };
    final DynamicType.Unloaded<?> avroSchemaClsUnloaded = new ByteBuddy()
            .rebase(Class.forName(avroSchemaClassName))
            .method(named("validateName").and(returns(String.class)).and(takesArguments(methArgTypesMatch))
                    .and(isPrivate()).and(isStatic()))
            .intercept(MethodDelegation.to(AvroSchemaInterceptor.class))
            .make();
    avroSchemaClsUnloaded.saveIn(avroSchemaClassesDirPrm);
  }

  private static String getClassRelativeFilePath(final String pckgName, final String name) {
    final String dotClsStr = ".class";
    //noinspection StringBufferReplaceableByString
    return new StringBuilder(name.length() + dotClsStr.length())
                   .append(pckgName.replace('.', File.separatorChar)).append(File.separatorChar)
                   .append(name.substring(pckgName.length() + 1)).append(dotClsStr)
                   .toString();
  }
}