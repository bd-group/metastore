/*
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
package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.GlobalMetaData;

/**
 * The input format to read a Parquet file.
 *
 * It requires an implementation of {@link ReadSupport} to materialize the records.
 *
 * The requestedSchema will control how the original records get projected by the loader.
 * It must be a subset of the original schema. Only the columns needed to reconstruct the records with the requestedSchema will be scanned.
 *
 * @see #READ_SUPPORT_CLASS
 * @see #UNBOUND_RECORD_FILTER
 * @see #STRICT_TYPE_CHECKING
 * @see #FILTER_PREDICATE
 * @see #TASK_SIDE_METADATA
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class LuquetInputFormat<T> extends FileInputFormat<Void, T> {
  /**
   * {@inheritDoc}
   */
  @Override
  public RecordReader<Void, T> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
	  //TODO:
	  return null;
  }



  /**
   * {@inheritDoc}
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
//    Configuration configuration = ContextUtil.getConfiguration(jobContext);
//    List<InputSplit> splits = new ArrayList<InputSplit>();
    //TODO:
    return null;
  }

  /**
   * @param configuration the configuration to connect to the file system
   * @param footers the footers of the files to read
   * @return the splits for the footers
   * @throws IOException
   * @deprecated split planning using file footers will be removed
   */
  @Deprecated
  public List<ParquetInputSplit> getSplits(Configuration configuration, List<Footer> footers) throws IOException {
    //TODO:
	  return null;
  }

  /**
   * @param jobContext the current job context
   * @return the footers for the files
   * @throws IOException
   */
  public List<Footer> getFooters(JobContext jobContext) throws IOException {
	 //TODO:
	  return null;
  }

  public List<Footer> getFooters(Configuration configuration, List<FileStatus> statuses) throws IOException {
	  //TODO:
	  return null;
  }

  /**
   * the footers for the files
   * @param configuration to connect to the file system
   * @param statuses the files to open
   * @return the footers of the files
   * @throws IOException
   */
  public List<Footer> getFooters(Configuration configuration, Collection<FileStatus> statuses) throws IOException {
	  //TODO:
	  return null;
  }

  /**
   * @param jobContext the current job context
   * @return the merged metadata from the footers
   * @throws IOException
   */
  public GlobalMetaData getGlobalMetaData(JobContext jobContext) throws IOException {
	  //TODO:
	  return null;
  }


}
