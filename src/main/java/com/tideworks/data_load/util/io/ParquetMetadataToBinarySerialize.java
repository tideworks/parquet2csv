/* ParquetMetadataToBinarySerialize.java
 *
 * Copyright June 2019 Tideworks Technology
 * Author: Roger D. Voss
 * MIT License
 */
package com.tideworks.data_load.util.io;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class ParquetMetadataToBinarySerialize {
  private static final Logger log = LoggerFactory.getLogger(ParquetMetadataToBinarySerialize.class.getSimpleName());

  public static void serializeFullFooter(final ParquetMetadata footer, final PositionOutputStream out)
        throws IOException
  {
    out.write(ParquetFileWriter.MAGIC);
    serializeFooter(footer, out, (fileMetadata, to, writeOut) -> writeOut.write(fileMetadata, to));
  }

  @FunctionalInterface
  private interface WriteFileMetaData {
    void write(org.apache.parquet.format.FileMetaData fileMetadata, OutputStream to) throws java.io.IOException;
  }

  @FunctionalInterface
  private interface CustomizeWriteFileMetaData {
    void customize(org.apache.parquet.format.FileMetaData fileMetadata, OutputStream to, WriteFileMetaData writeOut)
          throws java.io.IOException;
  }

  //
  // Based on:
  //  org.apache.parquet.hadoop.ParquetFileWriter.serializeFooter(ParquetMetadata footer, PositionOutputStream out)
  //    throws IOException;
  //
  // From:
  //  parquet-hadoop-1.10.0-sources.jar
  //
  private static void serializeFooter(final ParquetMetadata footer,
                                      final PositionOutputStream out,
                                      final CustomizeWriteFileMetaData writeCustomizer)
        throws IOException
  {
    final long footerIndex = out.getPos();
    final ParquetMetadataConverter metadataCnvtr = new ParquetMetadataConverter();
    final org.apache.parquet.format.FileMetaData parquetFileMetadata =
          metadataCnvtr.toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
    writeCustomizer.customize(parquetFileMetadata, out, Util::writeFileMetaData);
    log.debug("{}: footer length = {}", out.getPos(), (out.getPos() - footerIndex));
    BytesUtils.writeIntLittleEndian(out, (int) (out.getPos() - footerIndex));
    out.write(ParquetFileWriter.MAGIC);
  }
}
