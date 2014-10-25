/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.lib.query;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.lens.server.api.LensConfConstants;

/**
 * File output format which would write Text values in the charset enconding passed.
 *
 */
public class LensFileOutputFormat extends FileOutputFormat<NullWritable, Text> {

  /** The Constant UTF8. */
  public static final String UTF8 = "UTF-8";

  /** The Constant UTF16LE. */
  public static final String UTF16LE = "UTF-16LE";

  /** The Constant newline. */
  public static final String newline = "\n";

  /**
   * The Class LensRowWriter.
   */
  public static class LensRowWriter implements RecordWriter<NullWritable, Text> {

    /** The out. */
    protected OutputStreamWriter out;

    /** The tmp path. */
    private Path tmpPath;

    /** The extn. */
    private String extn;

    /**
     * Instantiates a new lens row writer.
     *
     * @param out
     *          the out
     * @param encoding
     *          the encoding
     * @param tmpPath
     *          the tmp path
     * @param extn
     *          the extn
     */
    public LensRowWriter(DataOutputStream out, String encoding, Path tmpPath, String extn) {
      this.tmpPath = tmpPath;
      this.extn = extn;
      try {
        this.out = new OutputStreamWriter(out, encoding);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + encoding + " encoding");
      }
    }

    /**
     * Instantiates a new lens row writer.
     *
     * @param out
     *          the out
     */
    public LensRowWriter(DataOutputStream out) {
      this(out, UTF8, null, null);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordWriter#write(java.lang.Object, java.lang.Object)
     */
    public synchronized void write(NullWritable key, Text value) throws IOException {
      boolean nullValue = value == null;
      if (!nullValue) {
        out.write(value.toString());
      }
      out.write(newline);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordWriter#close(org.apache.hadoop.mapred.Reporter)
     */
    public synchronized void close(Reporter reporter) throws IOException {
      if (out != null) {
        out.close();
      }
    }

    public Path getTmpPath() {
      return tmpPath;
    }

    public String getExtn() {
      return extn;
    }

    public String getEncoding() {
      return out.getEncoding();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.FileOutputFormat#getRecordWriter(org.apache.hadoop.fs.FileSystem,
   * org.apache.hadoop.mapred.JobConf, java.lang.String, org.apache.hadoop.util.Progressable)
   */
  public RecordWriter<NullWritable, Text> getRecordWriter(FileSystem ignored, JobConf job, String name,
      Progressable progress) throws IOException {
    return createRecordWriter(job, FileOutputFormat.getTaskOutputPath(job, name), progress, getCompressOutput(job),
        getOuptutFileExtn(job), getResultEncoding(job));
  }

  /**
   * Creates the record writer.
   *
   * @param conf
   *          the conf
   * @param tmpWorkPath
   *          the tmp work path
   * @param progress
   *          the progress
   * @param isCompressed
   *          the is compressed
   * @param extn
   *          the extn
   * @param encoding
   *          the encoding
   * @return the lens row writer
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public static LensRowWriter createRecordWriter(Configuration conf, Path tmpWorkPath, Progressable progress,
      boolean isCompressed, String extn, String encoding) throws IOException {
    Path file;
    if (extn != null) {
      file = new Path(tmpWorkPath + extn);
    } else {
      file = tmpWorkPath;
    }
    if (!isCompressed) {
      FileSystem fs = file.getFileSystem(conf);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new LensRowWriter(fileOut, encoding, file, extn);
    } else {
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(conf);
      // create the named codec
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
      // build the filename including the extension
      String codecExtn = codec.getDefaultExtension();
      file = new Path(file + codecExtn);
      FileSystem fs = file.getFileSystem(conf);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new LensRowWriter(new DataOutputStream(codec.createOutputStream(fileOut)), encoding, file, extn
          + codecExtn);
    }
  }

  /**
   * Gets the result encoding.
   *
   * @param conf
   *          the conf
   * @return the result encoding
   */
  public String getResultEncoding(Configuration conf) {
    return conf.get(LensConfConstants.QUERY_OUTPUT_CHARSET_ENCODING, LensConfConstants.DEFAULT_OUTPUT_CHARSET_ENCODING);
  }

  /**
   * Gets the ouptut file extn.
   *
   * @param conf
   *          the conf
   * @return the ouptut file extn
   */
  public String getOuptutFileExtn(Configuration conf) {
    return conf.get(LensConfConstants.QUERY_OUTPUT_FILE_EXTN, LensConfConstants.DEFAULT_OUTPUT_FILE_EXTN);
  }

  /**
   * Gets the output compressor class.
   *
   * @param conf
   *          the conf
   * @return the output compressor class
   */
  public static Class<? extends CompressionCodec> getOutputCompressorClass(Configuration conf) {
    Class<? extends CompressionCodec> codecClass;

    String name = conf.get(LensConfConstants.QUERY_OUTPUT_COMPRESSION_CODEC,
        LensConfConstants.DEFAULT_OUTPUT_COMPRESSION_CODEC);
    try {
      codecClass = conf.getClassByName(name).asSubclass(CompressionCodec.class);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Compression codec " + name + " was not found.", e);
    }
    return codecClass;
  }

  /**
   * Gets the compress output.
   *
   * @param conf
   *          the conf
   * @return the compress output
   */
  public static boolean getCompressOutput(Configuration conf) {
    return conf.getBoolean(LensConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION,
        LensConfConstants.DEFAULT_OUTPUT_ENABLE_COMPRESSION);
  }
}
