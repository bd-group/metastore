package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LuceneTableInputFormat extends InputFormat<Void, ArrayWritable> {

	@Override
	public RecordReader<Void, ArrayWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}