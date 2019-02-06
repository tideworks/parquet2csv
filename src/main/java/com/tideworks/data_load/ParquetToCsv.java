/* ParquetToCsv.java
 *
 * Copyright June 2018 Tideworks Technology
 * Author: Roger D. Voss
 * MIT License
 */
package com.tideworks.data_load;

import com.tideworks.data_load.io.BufferedWriterExt;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

import static com.tideworks.data_load.io.InputFile.nioPathToInputFile;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

class ParquetToCsv {
  private static final String csvDelimiter = ",";
  private static final Logger log = LoggerFactory.getLogger(ParquetToCsv.class.getSimpleName());
  private static final String fileExtent = ".parquet";
  private static final String notParquetFileErrMsgFmt =
          "\"{}\" does not end in '{}' - thus is not assumed to be a Parquet file and is being skipped";
  private static final String unsupportedFieldIndexErrMsgFmt = "Unsupported field index %d - %s only supports index 0";
  private static final String SPINNAKER_EPOC_START = "1900-01-01T00:00:00.000-00:00";
  private static final String MISC_DATETIME_PARSE_ERR = "1900-01-02T00:00:00.000-00:00";
  private static final DateTime spinnakerEpocStart;
  private static final DateTime miscDateTimeParseErr;

  static {
    final DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTime().withOffsetParsed();
    spinnakerEpocStart = dateTimeFormatter.parseDateTime(SPINNAKER_EPOC_START);
    miscDateTimeParseErr = dateTimeFormatter.parseDateTime(MISC_DATETIME_PARSE_ERR);
  }

  static void processToOutput(ZoneId timeZoneId, final File inputFile) throws IOException {
    final String fileName = inputFile.getName();
    if (!fileName.endsWith(fileExtent)) {
      log.error(notParquetFileErrMsgFmt, inputFile, fileExtent);
      return;
    }
    final int endIndex = fileName.lastIndexOf(fileExtent);
    final String fileNameBase = fileName.substring(0, endIndex);
    final Path csvOutputFilePath = Paths.get(inputFile.getParent(), fileNameBase + ".csv");

    final StringBuilder rowStrBuf = new StringBuilder(1024);
    final BiFunction<Schema.Field, Object, StringBuilder> fieldValueFormatter =
            makeFieldValueFormatter(timeZoneId, rowStrBuf);

    final Charset utf8 = Charset.forName("UTF-8");
    final int ioStreamBufSize = 16 * 1024;
    final OutputStream csvOutputStream = Files.newOutputStream(csvOutputFilePath, CREATE, TRUNCATE_EXISTING);
    final Writer outputWriter = new BufferedWriterExt(new OutputStreamWriter(csvOutputStream, utf8), ioStreamBufSize);

    Schema.Field[] fields = null;
    String[] fieldNames = new String[0];
    try (final Writer csvOutputWriter = outputWriter;
         final ParquetReader<GenericData.Record> reader = AvroParquetReader
                 .<GenericData.Record>builder(nioPathToInputFile(inputFile.toPath()))
                 .withConf(new Configuration())
                 .build())
    {
      GenericData.Record record;
      while ((record = reader.read()) != null) {
        if (fields == null) {
          final List<Schema.Field> fieldsList = record.getSchema().getFields();
          fieldNames = getFieldNames(fields = fieldsList.toArray(new Schema.Field[0]));
          csvOutputWriter.write(String.join(csvDelimiter, fieldNames));
          csvOutputWriter.write('\n');
        }
        rowStrBuf.setLength(0);
        int i = 0;
        for(final String fieldName : fieldNames) {
          fieldValueFormatter.apply(fields[i++], record.get(fieldName)).append(csvDelimiter);
        }
        rowStrBuf.deleteCharAt(rowStrBuf.length() - 1).append('\n');
        csvOutputWriter.append(rowStrBuf);
        csvOutputWriter.flush();
      }
      if (Files.size(csvOutputFilePath) <= 0) {
        Files.delete(csvOutputFilePath);
        log.warn("csv data file was empty (and was deleted): \"{}\"", csvOutputFilePath);
      }
    }
  }

  private static String[] getFieldNames(final Schema.Field[] fields) {
    final String[] fieldNames = new String[fields.length];
    int i = 0;
    for(final Schema.Field field : fields) {
      fieldNames[i++] = field.name().toUpperCase();
    }
    return fieldNames;
  }

  private static BiFunction<Schema.Field, Object, StringBuilder> makeFieldValueFormatter(final ZoneId timeZoneId,
                                                                                         final StringBuilder rowStrBuf)
  {
    final TimestampISO8601Format dateTimeFormatter = new TimestampISO8601Format(timeZoneId);
    final Conversions.DecimalConversion decimalConverter = new Conversions.DecimalConversion();
    final Conversions.UUIDConversion uuidLogicalTypeName = new Conversions.UUIDConversion();
    return (field, fieldValue) ->
               formatFieldValue(dateTimeFormatter, decimalConverter, uuidLogicalTypeName, rowStrBuf, field, fieldValue);
  }

  private static StringBuilder formatFieldValue(final TimestampISO8601Format dateTimeFormatter,
                                                final Conversions.DecimalConversion decimalConverter,
                                                final Conversions.UUIDConversion uuidLogicalTypeName,
                                                final StringBuilder rowStrBuf,
                                                final Schema.Field field,
                                                final Object fieldValue)
  {
    final Schema fieldSchema = field.schema();
    LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType == null) {
      Schema.Type fieldType = fieldSchema.getType();
      if (fieldType == Schema.Type.UNION) {
        fieldType = null;
        for(final Schema unionType : fieldSchema.getTypes()) {
          final LogicalType unionLogicalType = unionType.getLogicalType();
          if (unionLogicalType != null && logicalType == null) {
            logicalType = unionLogicalType;
            continue;
          }
          if (unionLogicalType == null && fieldType == null) {
            fieldType = unionType.getType();
          }
        }
      }
      if (logicalType == null) {
        if (fieldType != null) {
          switch (fieldType) {
            case RECORD:
              throw new UnsupportedOperationException();
            case ARRAY:
              throw new UnsupportedOperationException();
            case MAP:
              throw new UnsupportedOperationException();
            case UNION:
              throw new UnsupportedOperationException();
            case FIXED:
              throw new UnsupportedOperationException();
            case BYTES:
              throw new UnsupportedOperationException();
            default: {
              if (fieldValue != null) {
                switch (fieldType) {
                  case ENUM:
                  case STRING:
                    return rowStrBuf.append('\'').append(fieldValue).append('\'');
                  case INT:
                    break;
                  case LONG:
                    break;
                  case FLOAT: {
                    if (fieldValue instanceof Float) {
                      return rowStrBuf.append(new BigDecimal((Float) fieldValue));
                    } else if (fieldValue instanceof Double) {
                      return rowStrBuf.append(new BigDecimal((Double) fieldValue));
                    }
                    break;
                  }
                  case DOUBLE:
                    if (fieldValue instanceof Double) {
                      return rowStrBuf.append(new BigDecimal((Double) fieldValue));
                    }
                  case BOOLEAN:
                    break;
                  case NULL:
                    break;
                }
              }
            }
          }
        }
        return rowStrBuf.append(fieldValue);
      }
    }
    if (fieldValue == null) {
      return rowStrBuf.append("null");
    }
    if (logicalType instanceof LogicalTypes.Date ||
        logicalType instanceof LogicalTypes.TimestampMillis ||
        logicalType instanceof LogicalTypes.TimeMillis)
    {
      final long epocTimeMS = (Long) fieldValue;
      String fmtDateTimeText;
      if (epocTimeMS == spinnakerEpocStart.getMillis()) {
        fmtDateTimeText = SPINNAKER_EPOC_START;
      } else if (epocTimeMS == miscDateTimeParseErr.getMillis()) {
        fmtDateTimeText = MISC_DATETIME_PARSE_ERR;
      } else {
        fmtDateTimeText = dateTimeFormatter.format(new Date(epocTimeMS));
      }
      rowStrBuf.append('\'').append(fmtDateTimeText).append('\'');
    } else if (logicalType instanceof LogicalTypes.Decimal) {
      final ByteBuffer byteBufFieldValue = (ByteBuffer) fieldValue;
      final BigDecimal bigDecimalFieldValue = decimalConverter.fromBytes(byteBufFieldValue, fieldSchema, logicalType);
      rowStrBuf.append(bigDecimalFieldValue);
    } else if (logicalType.getName().equals(uuidLogicalTypeName.getLogicalTypeName())) {
      final CharSequence csFieldValue = (CharSequence) fieldValue;
      try {
        final UUID uuidFieldValue = uuidLogicalTypeName.fromCharSequence(csFieldValue, fieldSchema, logicalType);
        rowStrBuf.append('\'').append(uuidFieldValue).append('\'');
      } catch(IllegalArgumentException e) {
        String unkwn = "unknown";
        log.warn("{}: {} - using substitute UUID value: \"{}\"", e.getClass().getSimpleName(), e.getMessage(), unkwn);
        rowStrBuf.append('\'').append(unkwn).append('\'');
      }
    } else {
      rowStrBuf.append(fieldValue);
    }
    return rowStrBuf;
  }

  @SuppressWarnings("unused")
  private static final class TimestampISO8601Format extends DateFormat {
    private final ZoneId timeZoneID;

    TimestampISO8601Format(ZoneId timeZoneID) {
      this.timeZoneID = timeZoneID;
    }
    TimestampISO8601Format() {
      this(ZoneId.systemDefault());
    }
    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
      if (date == null) return toAppendTo;
      final int fieldIndex = fieldPosition.getField();
      if (fieldIndex == 0) {
        @SuppressWarnings("RedundantCast")
        final Instant instant = date instanceof Timestamp ? ((Timestamp) date).toInstant() : date.toInstant();
        return toAppendTo.append(OffsetDateTime.ofInstant(instant, timeZoneID));
      } else {
        throw new AssertionError(String.format(unsupportedFieldIndexErrMsgFmt, fieldIndex, getClass().getSimpleName()));
      }
    }
    @Override
    public Date parse(String source, ParsePosition pos) {
      throw new UnsupportedOperationException();
    }
  }
}