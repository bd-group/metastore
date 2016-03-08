
package org.apache.hadoop.hive.ql.io;

import static org.apache.parquet.Log.INFO;
import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;
import static org.apache.parquet.hadoop.util.ContextUtil.getConfiguration;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.Log;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.MemoryManager;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.codec.CodecConfig;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ConfigurationUtil;

/**
 * OutputFormat to write to a Parquet file
 *
 * It requires a {@link WriteSupport} to convert the actual records to the underlying format.
 * It requires the schema of the incoming records. (provided by the write support)
 * It allows storing extra metadata in the footer (for example: for schema compatibility purpose when converting from a different schema language).
 *
 * The format configuration settings in the job configuration:
 * <pre>
 * # The block size is the size of a row group being buffered in memory
 * # this limits the memory usage when writing
 * # Larger values will improve the IO when reading but consume more memory when writing
 * parquet.block.size=134217728 # in bytes, default = 128 * 1024 * 1024
 *
 * # The page size is for compression. When reading, each page can be decompressed independently.
 * # A block is composed of pages. The page is the smallest unit that must be read fully to access a single record.
 * # If this value is too small, the compression will deteriorate
 * parquet.page.size=1048576 # in bytes, default = 1 * 1024 * 1024
 *
 * # There is one dictionary page per column per row group when dictionary encoding is used.
 * # The dictionary page size works like the page size but for dictionary
 * parquet.dictionary.page.size=1048576 # in bytes, default = 1 * 1024 * 1024
 *
 * # The compression algorithm used to compress pages
 * parquet.compression=UNCOMPRESSED # one of: UNCOMPRESSED, SNAPPY, GZIP, LZO. Default: UNCOMPRESSED. Supersedes mapred.output.compress*
 *
 * # The write support class to convert the records written to the OutputFormat into the events accepted by the record consumer
 * # Usually provided by a specific ParquetOutputFormat subclass
 * parquet.write.support.class= # fully qualified name
 *
 * # To enable/disable dictionary encoding
 * parquet.enable.dictionary=true # false to disable dictionary encoding
 *
 * # To enable/disable summary metadata aggregation at the end of a MR job
 * # The default is true (enabled)
 * parquet.enable.summary-metadata=true # false to disable summary aggregation
 *
 * # Maximum size (in bytes) allowed as padding to align row groups
 * # This is also the minimum size of a row group. Default: 0
 * parquet.writer.max-padding=2097152 # 2 MB
 * </pre>
 *
 * If parquet.compression is not set, the following properties are checked (FileOutputFormat behavior).
 * Note that we explicitely disallow custom Codecs
 * <pre>
 * mapred.output.compress=true
 * mapred.output.compression.codec=org.apache.hadoop.io.compress.SomeCodec # the codec must be one of Snappy, GZip or LZO
 * </pre>
 *
 * if none of those is set the data is uncompressed.
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class LuquetOutputFormat<T> extends FileOutputFormat<Void,T> implements HiveOutputFormat<Void,T> {
  private static final Log LOG = Log.getLog(ParquetOutputFormat.class);

  public static final String BLOCK_SIZE           = "parquet.block.size";
  public static final String PAGE_SIZE            = "parquet.page.size";
  public static final String COMPRESSION          = "parquet.compression";
  public static final String WRITE_SUPPORT_CLASS  = "parquet.write.support.class";
  public static final String DICTIONARY_PAGE_SIZE = "parquet.dictionary.page.size";
  public static final String ENABLE_DICTIONARY    = "parquet.enable.dictionary";
  public static final String VALIDATION           = "parquet.validation";
  public static final String WRITER_VERSION       = "parquet.writer.version";
  public static final String ENABLE_JOB_SUMMARY   = "parquet.enable.summary-metadata";
  public static final String MEMORY_POOL_RATIO    = "parquet.memory.pool.ratio";
  public static final String MIN_MEMORY_ALLOCATION = "parquet.memory.min.chunk.size";
  public static final String MAX_PADDING_BYTES    = "parquet.writer.max-padding";

  // default to no padding for now
  private static final int DEFAULT_MAX_PADDING_SIZE = 0;

  public static void setWriteSupportClass(Job job,  Class<?> writeSupportClass) {
    getConfiguration(job).set(WRITE_SUPPORT_CLASS, writeSupportClass.getName());
  }

  public static void setWriteSupportClass(JobConf job, Class<?> writeSupportClass) {
      job.set(WRITE_SUPPORT_CLASS, writeSupportClass.getName());
  }

  public static Class<?> getWriteSupportClass(Configuration configuration) {
    final String className = configuration.get(WRITE_SUPPORT_CLASS);
    if (className == null) {
      return null;
    }
    final Class<?> writeSupportClass = ConfigurationUtil.getClassFromConfig(configuration, WRITE_SUPPORT_CLASS, WriteSupport.class);
    return writeSupportClass;
  }

  public static void setBlockSize(Job job, int blockSize) {
    getConfiguration(job).setInt(BLOCK_SIZE, blockSize);
  }

  public static void setPageSize(Job job, int pageSize) {
    getConfiguration(job).setInt(PAGE_SIZE, pageSize);
  }

  public static void setDictionaryPageSize(Job job, int pageSize) {
    getConfiguration(job).setInt(DICTIONARY_PAGE_SIZE, pageSize);
  }

  public static void setCompression(Job job, CompressionCodecName compression) {
    getConfiguration(job).set(COMPRESSION, compression.name());
  }

  public static void setEnableDictionary(Job job, boolean enableDictionary) {
    getConfiguration(job).setBoolean(ENABLE_DICTIONARY, enableDictionary);
  }

  public static boolean getEnableDictionary(JobContext jobContext) {
    return getEnableDictionary(getConfiguration(jobContext));
  }

  public static int getBlockSize(JobContext jobContext) {
    return getBlockSize(getConfiguration(jobContext));
  }

  public static int getPageSize(JobContext jobContext) {
    return getPageSize(getConfiguration(jobContext));
  }

  public static int getDictionaryPageSize(JobContext jobContext) {
    return getDictionaryPageSize(getConfiguration(jobContext));
  }

  public static CompressionCodecName getCompression(JobContext jobContext) {
    return getCompression(getConfiguration(jobContext));
  }

  public static boolean isCompressionSet(JobContext jobContext) {
    return isCompressionSet(getConfiguration(jobContext));
  }

  public static void setValidation(JobContext jobContext, boolean validating) {
    setValidation(getConfiguration(jobContext), validating);
  }

  public static boolean getValidation(JobContext jobContext) {
    return getValidation(getConfiguration(jobContext));
  }

  public static boolean getEnableDictionary(Configuration configuration) {
    return configuration.getBoolean(ENABLE_DICTIONARY, true);
  }

  @Deprecated
  public static int getBlockSize(Configuration configuration) {
    return configuration.getInt(BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
  }

  public static long getLongBlockSize(Configuration configuration) {
    return configuration.getLong(BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
  }

  public static int getPageSize(Configuration configuration) {
    return configuration.getInt(PAGE_SIZE, DEFAULT_PAGE_SIZE);
  }

  public static int getDictionaryPageSize(Configuration configuration) {
    return configuration.getInt(DICTIONARY_PAGE_SIZE, DEFAULT_PAGE_SIZE);
  }

  public static WriterVersion getWriterVersion(Configuration configuration) {
    String writerVersion = configuration.get(WRITER_VERSION, WriterVersion.PARQUET_1_0.toString());
    return WriterVersion.fromString(writerVersion);
  }

  public static CompressionCodecName getCompression(Configuration configuration) {
    return CodecConfig.getParquetCompressionCodec(configuration);
  }

  public static boolean isCompressionSet(Configuration configuration) {
    return CodecConfig.isParquetCompressionSet(configuration);
  }

  public static void setValidation(Configuration configuration, boolean validating) {
    configuration.setBoolean(VALIDATION, validating);
  }

  public static boolean getValidation(Configuration configuration) {
    return configuration.getBoolean(VALIDATION, false);
  }

  private CompressionCodecName getCodec(TaskAttemptContext taskAttemptContext) {
    return CodecConfig.from(taskAttemptContext).getCodec();
  }

  public static void setMaxPaddingSize(JobContext jobContext, int maxPaddingSize) {
    setMaxPaddingSize(getConfiguration(jobContext), maxPaddingSize);
  }

  public static void setMaxPaddingSize(Configuration conf, int maxPaddingSize) {
    conf.setInt(MAX_PADDING_BYTES, maxPaddingSize);
  }

  private static int getMaxPaddingSize(Configuration conf) {
    // default to no padding, 0% of the row group size
    return conf.getInt(MAX_PADDING_BYTES, DEFAULT_MAX_PADDING_SIZE);
  }


  private WriteSupport<T> writeSupport;

  /**
   * constructor used when this OutputFormat in wrapped in another one (In Pig for example)
   * @param writeSupportClass the class used to convert the incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to be stored in the footer of the file
   */
  public <S extends WriteSupport<T>> LuquetOutputFormat(S writeSupport) {
    this.writeSupport = writeSupport;
  }

  /**
   * used when directly using the output format and configuring the write support implementation
   * using parquet.write.support.class
   */
  public <S extends WriteSupport<T>> LuquetOutputFormat() {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {

    final Configuration conf = getConfiguration(taskAttemptContext);

    CompressionCodecName codec = getCodec(taskAttemptContext);
    String extension = codec.getExtension() + ".parquet";
    Path file = getDefaultWorkFile(taskAttemptContext, extension);
    return getRecordWriter(conf, file, codec);
  }

  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext, Path file)
      throws IOException, InterruptedException {
    return getRecordWriter(getConfiguration(taskAttemptContext), file, getCodec(taskAttemptContext));
  }

  public RecordWriter<Void, T> getRecordWriter(Configuration conf, Path file, CompressionCodecName codec)
        throws IOException, InterruptedException {
    final WriteSupport<T> writeSupport = getWriteSupport(conf);

    CodecFactory codecFactory = new CodecFactory(conf);
    long blockSize = getLongBlockSize(conf);
    if (INFO) {
      LOG.info("Parquet block size to " + blockSize);
    }
    int pageSize = getPageSize(conf);
    if (INFO) {
      LOG.info("Parquet page size to " + pageSize);
    }
    int dictionaryPageSize = getDictionaryPageSize(conf);
    if (INFO) {
      LOG.info("Parquet dictionary page size to " + dictionaryPageSize);
    }
    boolean enableDictionary = getEnableDictionary(conf);
    if (INFO) {
      LOG.info("Dictionary is " + (enableDictionary ? "on" : "off"));
    }
    boolean validating = getValidation(conf);
    if (INFO) {
      LOG.info("Validation is " + (validating ? "on" : "off"));
    }
    WriterVersion writerVersion = getWriterVersion(conf);
    if (INFO) {
      LOG.info("Writer version is: " + writerVersion);
    }
    int maxPaddingSize = getMaxPaddingSize(conf);
    if (INFO) {
      LOG.info("Maximum row group padding size is " + maxPaddingSize + " bytes");
    }

    WriteContext init = writeSupport.init(conf);
    ParquetFileWriter w = new ParquetFileWriter(
        conf, init.getSchema(), file, Mode.CREATE, blockSize, maxPaddingSize);
    w.start();


    return new ParquetRecordWriter<T>(
        w,
        writeSupport,
        init.getSchema(),
        init.getExtraMetaData(),
        blockSize, pageSize,
        codecFactory.getCompressor(codec, pageSize),
        dictionaryPageSize,
        enableDictionary,
        validating,
        writerVersion,
        memoryManager);
  }

  /**
   * @param configuration to find the configuration for the write support class
   * @return the configured write support
   */
  @SuppressWarnings("unchecked")
  public WriteSupport<T> getWriteSupport(Configuration configuration){
    if (writeSupport != null) {
      return writeSupport;
    }
    Class<?> writeSupportClass = getWriteSupportClass(configuration);
    try {
      return (WriteSupport<T>)checkNotNull(writeSupportClass, "writeSupportClass").newInstance();
    } catch (InstantiationException e) {
      throw new BadConfigurationException("could not instantiate write support class: " + writeSupportClass, e);
    } catch (IllegalAccessException e) {
      throw new BadConfigurationException("could not instantiate write support class: " + writeSupportClass, e);
    }
  }



  /**
   * This memory manager is for all the real writers (InternalParquetRecordWriter) in one task.
   */
  private static MemoryManager memoryManager;

  public static MemoryManager getMemoryManager() {
    return memoryManager;
  }

@Override
public void checkOutputSpecs(FileSystem arg0, JobConf arg1) throws IOException {
	// TODO Auto-generated method stub

}

@Override
public org.apache.hadoop.mapred.RecordWriter<Void, T> getRecordWriter(
		FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
		throws IOException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
		JobConf arg0, Path arg1, Class<? extends Writable> arg2, boolean arg3,
		Properties arg4, Progressable arg5) throws IOException {
	// TODO Auto-generated method stub
	return null;
}
}