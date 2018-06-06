package com.tideworks.data_load.io;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;

public class BufferedWriterExt extends BufferedWriter {

  public BufferedWriterExt(Writer out) {
    super(out);
  }

  public BufferedWriterExt(Writer out, int sz) {
    super(out, sz);
  }

  @Override
  public Writer append(CharSequence csq) throws IOException {
    return this.append(csq, 0, csq.length());
  }

  /**
   * This override implementation avoids the unnecessary buffer allocation
   * overhead of the standard implementation which is equivalent to:
   * <p>
   * write(csq.subsequence(start, end).toString()); // causes String allocation
   * <p>
   * This implementation instead writes data directly from the source
   * {@link CharSequence} buffer to the output writer.
   *
   * @param csq character sequence of which a subsequence will be appended to output
   * @param start starting index of subsequence
   * @param end ending index following last character of subsequence
   * @return This Writer
   * @throws IOException
   */
  @Override
  public Writer append(CharSequence csq, int start, int end) throws IOException {
    int len = csq.length();
    if (end > len)
      len = end;

    for(int i = start; i < len; i++) {
      this.write(csq.charAt(i));
    }

    return this;
  }
}
