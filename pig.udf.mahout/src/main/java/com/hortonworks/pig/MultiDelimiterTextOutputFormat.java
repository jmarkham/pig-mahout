package com.hortonworks.pig;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.StorageUtil;

@SuppressWarnings("rawtypes")
public class MultiDelimiterTextOutputFormat extends
		TextOutputFormat<WritableComparable, Tuple> {

	private final String fieldDel;

	protected static class MultiDelimiterRecordWriter extends
			TextOutputFormat.LineRecordWriter<WritableComparable, Tuple> {
		private static final String utf8 = "UTF-8";
		private static final byte[] newline;
		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		private final String fieldDel;

		public MultiDelimiterRecordWriter(DataOutputStream out, String fieldDel) {
			super(out);
			this.fieldDel = fieldDel;
		}

		public synchronized void write(WritableComparable key, Tuple value)
				throws IOException {
			int sz = value.size();
			for (int i = 0; i < sz; i++) {
				StorageUtil.putField(out, value.get(i));
				if (i != sz - 1) {
					out.writeBytes(fieldDel);
				}
			}
			out.write(newline);
		}
	}

	public MultiDelimiterTextOutputFormat(String delimiter) {
		super();
		fieldDel = delimiter;
	}

	@Override
	public RecordWriter<WritableComparable, Tuple> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);
		if (!isCompressed) {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new MultiDelimiterRecordWriter(fileOut, fieldDel);
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new MultiDelimiterRecordWriter(new DataOutputStream(
					codec.createOutputStream(fileOut)), fieldDel);
		}
	}
}