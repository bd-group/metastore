/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

/**
 *
 * A Parquet OutputFormat for Hive (with the deprecated package mapred)
 *
 */
public class MapredParquetOutputFormat extends FileOutputFormat<Void, ArrayWritable> implements
  HiveOutputFormat<Void, ArrayWritable> {


  @Override
  public void checkOutputSpecs(final FileSystem ignored, final JobConf job) throws IOException {

  }

  @Override
  public RecordWriter<Void, ArrayWritable> getRecordWriter(
      final FileSystem ignored,
      final JobConf job,
      final String name,
      final Progressable progress
      ) throws IOException {
    throw new RuntimeException("Should never be used");
  }

  /**
   *
   * Create the parquet schema from the hive schema, and return the RecordWriterWrapper which
   * contains the real output format
   */
  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
      final JobConf jobConf,
      final Path finalOutPath,
      final Class<? extends Writable> valueClass,
      final boolean isCompressed,
      final Properties tableProperties,
      final Progressable progress) throws IOException {

      return null;
  }

}
