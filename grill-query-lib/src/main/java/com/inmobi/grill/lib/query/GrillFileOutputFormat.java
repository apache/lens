package com.inmobi.grill.lib.query;

/*
 * #%L
 * Grill Query Library
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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

import com.inmobi.grill.server.api.GrillConfConstants;

/**
 * File output format which would write Text values in the charset enconding 
 * passed.
 *
 */
public class GrillFileOutputFormat extends FileOutputFormat<NullWritable, Text> {

  public static final String UTF8 = "UTF-8";
  public static final String UTF16LE = "UTF-16LE";
  public static final String newline  = "\n";

  public static class GrillRowWriter implements RecordWriter<NullWritable, Text> {
    protected OutputStreamWriter out;
    private Path tmpPath;
    private String extn;

    public GrillRowWriter(DataOutputStream out, String encoding, Path tmpPath, String extn) {
      this.tmpPath = tmpPath;
      this.extn = extn;
      try {
        this.out = new OutputStreamWriter(out, encoding);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + encoding + " encoding");
      }
    }

    public GrillRowWriter(DataOutputStream out) {
      this(out, UTF8, null, null);
    }

    public synchronized void write(NullWritable key, Text value)
        throws IOException {
      boolean nullValue = value == null;
      if (!nullValue) {
        out.write(value.toString());
      }
      out.write(newline);
    }

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

  public RecordWriter<NullWritable, Text> getRecordWriter(FileSystem ignored,
      JobConf job,
      String name,
      Progressable progress)
          throws IOException {
    return createRecordWriter(job,
        FileOutputFormat.getTaskOutputPath(job, name), progress,
        getCompressOutput(job),
        getOuptutFileExtn(job), getResultEncoding(job));
  }

  public static GrillRowWriter createRecordWriter(Configuration conf,
      Path tmpWorkPath, Progressable progress, boolean isCompressed, String extn,
      String encoding) throws IOException {
    Path file;
    if (extn != null) {
      file = new Path (tmpWorkPath + extn);
    } else {
      file = tmpWorkPath;
    }
    if (!isCompressed) {
      FileSystem fs = file.getFileSystem(conf);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new GrillRowWriter(fileOut, encoding, file, extn);
    } else {
      Class<? extends CompressionCodec> codecClass =
          getOutputCompressorClass(conf);
      // create the named codec
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
      // build the filename including the extension
      String codecExtn = codec.getDefaultExtension();
      file = new Path (file + codecExtn);
      FileSystem fs = file.getFileSystem(conf);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new GrillRowWriter(new DataOutputStream
          (codec.createOutputStream(fileOut)),
          encoding, file, extn + codecExtn);
    }
  }

  public String getResultEncoding(Configuration conf) {
    return conf.get(GrillConfConstants.QUERY_OUTPUT_CHARSET_ENCODING,
        GrillConfConstants.DEFAULT_OUTPUT_CHARSET_ENCODING);
  }

  public String getOuptutFileExtn(Configuration conf) {
    return conf.get(GrillConfConstants.QUERY_OUTPUT_FILE_EXTN,
        GrillConfConstants.DEFAULT_OUTPUT_FILE_EXTN);
  }

  public static Class<? extends CompressionCodec> getOutputCompressorClass(
      Configuration conf) {
    Class<? extends CompressionCodec> codecClass;

    String name = conf.get(GrillConfConstants.QUERY_OUTPUT_COMPRESSION_CODEC,
        GrillConfConstants.DEFAULT_OUTPUT_COMPRESSION_CODEC);
    try {
      codecClass = 
          conf.getClassByName(name).asSubclass(CompressionCodec.class);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Compression codec " + name + 
          " was not found.", e);
    }
    return codecClass;
  }

  public static boolean getCompressOutput(Configuration conf) {
    return conf.getBoolean(GrillConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION,
        GrillConfConstants.DEFAULT_OUTPUT_ENABLE_COMPRESSION);
  }
}
