package org.apache.lens.lib.query;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Zip file formatter.
 *
 * Creates a zip on hadoop compatible file system, with ability to split output across multiple part files and provide a
 * final zip output file.
 *
 */
public class ZipFileFormatter extends AbstractFileFormatter {

  /** The part suffix. */
  public static String PART_SUFFIX = "_part-";

  /** The tmp path. */
  private Path tmpPath;

  /** The zip out. */
  private ZipOutputStream zipOut;

  /** The fs. */
  private FileSystem fs;

  /** The result file extn. */
  private String resultFileExtn;

  /** The current part. */
  private int currentPart = 0;

  /** The out. */
  private OutputStreamWriter out;

  /** The max split rows. */
  private long maxSplitRows;

  /** The encoding. */
  private String encoding;

  /** The closed. */
  boolean closed = false;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.lib.query.FileFormatter#setupOutputs()
   */
  public void setupOutputs() throws IOException {
    resultFileExtn = ctx.getOuptutFileExtn();
    maxSplitRows = ctx.getMaxResultSplitRows();

    String pathStr = ctx.getResultSetParentDir();
    if (StringUtils.isBlank(pathStr)) {
      throw new IllegalArgumentException("No output path specified");
    }
    finalPath = new Path(pathStr, ctx.getQueryHandle().toString() + ".zip");
    tmpPath = new Path(pathStr, ctx.getQueryHandle().toString() + ".tmp.zip");

    fs = finalPath.getFileSystem(ctx.getConf());

    zipOut = new ZipOutputStream((fs.create(tmpPath)));
    ZipEntry zipEntry = new ZipEntry(getQueryResultFileName());
    zipOut.putNextEntry(zipEntry);
    encoding = ctx.getResultEncoding();
    // Write the UTF-16LE BOM (FF FE)
    if (encoding.equals(LensFileOutputFormat.UTF16LE)) {
      zipOut.write(0xFF);
      zipOut.write(0xFE);
      out = new OutputStreamWriter(zipOut, encoding);
    } else {
      out = new OutputStreamWriter(zipOut, encoding);
    }
    System.out.println("Setup outputs done");
  }

  private String getQueryResultFileName() {
    return ctx.getQueryHandle().toString() + PART_SUFFIX + currentPart + resultFileExtn;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryOutputFormatter#commit()
   */
  @Override
  public void commit() throws IOException {
    close();
    fs.rename(tmpPath, finalPath);
    finalPath = finalPath.makeQualified(fs);
    ctx.setResultSetPath(getFinalOutputPath());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryOutputFormatter#close()
   */
  @Override
  public void close() throws IOException {
    if (!closed) {
      out.flush();
      zipOut.closeEntry();
      zipOut.close();
      out.close();
      closed = true;
    }
  }

  /** The cached header. */
  private String cachedHeader = null;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.lib.query.FileFormatter#writeHeader(java.lang.String)
   */
  public void writeHeader(String header) throws IOException {
    out.write(header);
    out.write("\n");
    this.cachedHeader = header;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.lib.query.FileFormatter#writeFooter(java.lang.String)
   */
  public void writeFooter(String footer) throws IOException {
    out.write(footer);
    out.write("\n");
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.lib.query.FileFormatter#writeRow(java.lang.String)
   */
  public void writeRow(String row) throws IOException {
    // close zip entry and add new one, if numRows has crossed max rows in the
    // cuurent file
    if (numRows != 0 && numRows % maxSplitRows == 0) {
      currentPart++;
      out.flush();
      zipOut.closeEntry();

      // Making new zip-entry.
      ZipEntry zipEntry = new ZipEntry(getQueryResultFileName());
      zipOut.putNextEntry(zipEntry);
      if (encoding.equals(LensFileOutputFormat.UTF16LE)) {
        zipOut.write(0xFF);
        zipOut.write(0xFE);
      }
      writeHeader();
    }
    out.write(row);
    out.write("\n");
    numRows++;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.lib.query.AbstractFileFormatter#writeHeader()
   */
  @Override
  public void writeHeader() throws IOException {
    if (cachedHeader != null) {
      writeHeader(cachedHeader);
    }
  }

  @Override
  public Path getTmpPath() {
    return tmpPath;
  }

  @Override
  public String getEncoding() {
    return out.getEncoding();
  }

}
