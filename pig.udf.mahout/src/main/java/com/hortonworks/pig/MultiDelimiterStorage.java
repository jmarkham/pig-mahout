package com.hortonworks.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.builtin.JsonMetadata;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.CastUtils;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

public class MultiDelimiterStorage extends FileInputLoadFunc implements
		StoreFuncInterface, LoadPushDown, LoadMetadata, StoreMetadata {

	@SuppressWarnings("rawtypes")
	protected RecordReader in = null;
	@SuppressWarnings("rawtypes")
	protected RecordWriter writer = null;
	protected final Log mLog = LogFactory.getLog(getClass());
	protected String signature;

	private String fieldDel = "\t";
	private ArrayList<Object> mProtoTuple = null;
	private TupleFactory mTupleFactory = TupleFactory.getInstance();
	private String loadLocation;

	boolean isSchemaOn = false;
	boolean dontLoadSchema = false;
	protected ResourceSchema schema;
	protected LoadCaster caster;

	private final CommandLine configuredOptions;
	private final Options validOptions = new Options();
	private final static CommandLineParser parser = new GnuParser();

	protected boolean[] mRequiredColumns = null;
	private boolean mRequiredColumnsInitialized = false;

	// Indicates whether the input file path should be read.
	private boolean tagSource = false;
	private static final String TAG_SOURCE_PATH = "tagsource";
	private Path sourcePath = null;

	private void populateValidOptions() {
		validOptions
				.addOption("schema", false,
						"Loads / Stores the schema of the relation using a hidden JSON file.");
		validOptions.addOption("noschema", false,
				"Disable attempting to load data schema from the filesystem.");
		validOptions
				.addOption(
						TAG_SOURCE_PATH,
						false,
						"Appends input source file path to end of each tuple. Make sure to set pig.splitCombination to false");
	}

	public MultiDelimiterStorage() {
		this("\t", "");
	}

	/**
	 * Constructs a Pig loader that uses specified character as a field
	 * delimiter.
	 * 
	 * @param delimiter
	 *            the single byte character that is used to separate fields.
	 *            ("\t" is the default.)
	 * @throws ParseException
	 */
	public MultiDelimiterStorage(String delimiter) {
		this(delimiter, "");
	}

	/**
	 * Constructs a Pig loader that uses specified character as a field
	 * delimiter.
	 * <p>
	 * Understands the following options, which can be specified in the second
	 * paramter:
	 * <ul>
	 * <li><code>-schema</code> Loads / Stores the schema of the relation using
	 * a hidden JSON file.
	 * <li><code>-noschema</code> Ignores a stored schema during loading.
	 * <li><code>-tagsource</code> Appends input source file path to end of each
	 * tuple. Make sure to set "pig.splitCombination" to false
	 * </ul>
	 * 
	 * @param delimiter
	 *            the single byte character that is used to separate fields.
	 * @param options
	 *            a list of options that can be used to modify PigStorage
	 *            behavior
	 * @throws ParseException
	 */
	public MultiDelimiterStorage(String delimiter, String options) {
		populateValidOptions();
		fieldDel = parseFieldDel(delimiter);
		String[] optsArr = options.split(" ");
		try {
			configuredOptions = parser.parse(validOptions, optsArr);
			isSchemaOn = configuredOptions.hasOption("schema");
			dontLoadSchema = configuredOptions.hasOption("noschema");
			tagSource = configuredOptions.hasOption(TAG_SOURCE_PATH);
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("PigStorage(',', '[options]')", validOptions);
			// We wrap this exception in a Runtime exception so that
			// existing loaders that extend PigStorage don't break
			throw new RuntimeException(e);
		}
	}

	public String parseFieldDel(String delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Null delimiter");
		}
		int startIndex = 0;
		int endIndex;
		while (startIndex < delimiter.length()
				&& delimiter.charAt(startIndex++) != '\'')
			;
		endIndex = startIndex;
		while (endIndex < delimiter.length()
				&& delimiter.charAt(endIndex) != '\'') {
			if (delimiter.charAt(endIndex) == '\\') {
				endIndex++;
			}
			endIndex++;
		}

		return (endIndex < delimiter.length()) ? delimiter.substring(
				startIndex, endIndex) : delimiter;
	}

	@Override
	public Tuple getNext() throws IOException {
		mProtoTuple = new ArrayList<Object>();
		if (!mRequiredColumnsInitialized) {
			if (signature != null) {
				Properties p = UDFContext.getUDFContext().getUDFProperties(
						this.getClass());
				mRequiredColumns = (boolean[]) ObjectSerializer.deserialize(p
						.getProperty(signature));
			}
			mRequiredColumnsInitialized = true;
		}
		// Prepend input source path if source tagging is enabled
		if (tagSource) {
			mProtoTuple.add(new DataByteArray(sourcePath.getName()));
		}

		try {
			boolean notDone = in.nextKeyValue();
			if (!notDone) {
				return null;
			}
			Text value = (Text) in.getCurrentValue();
			String record = new String(value.getBytes());

			int fieldID = 0;
			for (String field : record.split(fieldDel)) {
				if (mRequiredColumns == null
						|| (mRequiredColumns.length > fieldID && mRequiredColumns[fieldID])) {
					if (field.isEmpty()) {
						mProtoTuple.add(null);
					} else {
						mProtoTuple.add(new DataByteArray(field));
					}
				}
				fieldID++;
			}

			Tuple t = mTupleFactory.newTupleNoCopy(mProtoTuple);

			return dontLoadSchema ? t : applySchema(t);
		} catch (InterruptedException e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}
	}

	private Tuple applySchema(Tuple tup) throws IOException {
		if (caster == null) {
			caster = getLoadCaster();
		}
		if (signature != null && schema == null) {
			Properties p = UDFContext.getUDFContext().getUDFProperties(
					this.getClass(), new String[] { signature });
			String serializedSchema = p.getProperty(signature + ".schema");
			if (serializedSchema == null)
				return tup;
			try {
				schema = new ResourceSchema(
						Utils.getSchemaFromString(serializedSchema));
			} catch (ParserException e) {
				mLog.error("Unable to parse serialized schema "
						+ serializedSchema, e);
			}
		}

		if (schema != null) {

			ResourceFieldSchema[] fieldSchemas = schema.getFields();
			int tupleIdx = 0;
			// If some fields have been projected out, the tuple
			// only contains required fields.
			// We walk the requiredColumns array to find required fields,
			// and cast those.
			for (int i = 0; i < fieldSchemas.length; i++) {
				if (mRequiredColumns == null
						|| (mRequiredColumns.length > i && mRequiredColumns[i])) {
					Object val = null;
					if (tup.get(tupleIdx) != null) {
						byte[] bytes = ((DataByteArray) tup.get(tupleIdx))
								.get();
						val = CastUtils.convertToType(caster, bytes,
								fieldSchemas[i], fieldSchemas[i].getType());
					}
					tup.set(tupleIdx, val);
					tupleIdx++;
				}
			}
		}
		return tup;
	}

	@SuppressWarnings("unchecked")
	public void putNext(Tuple f) throws IOException {
		try {
			writer.write(null, f);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	public RequiredFieldResponse pushProjection(
			RequiredFieldList requiredFieldList) throws FrontendException {
		if (requiredFieldList == null)
			return null;
		if (requiredFieldList.getFields() != null) {
			int lastColumn = -1;
			for (RequiredField rf : requiredFieldList.getFields()) {
				if (rf.getIndex() > lastColumn) {
					lastColumn = rf.getIndex();
				}
			}
			mRequiredColumns = new boolean[lastColumn + 1];
			for (RequiredField rf : requiredFieldList.getFields()) {
				if (rf.getIndex() != -1)
					mRequiredColumns[rf.getIndex()] = true;
			}
			Properties p = UDFContext.getUDFContext().getUDFProperties(
					this.getClass());
			try {
				p.setProperty(signature,
						ObjectSerializer.serialize(mRequiredColumns));
			} catch (Exception e) {
				throw new RuntimeException("Cannot serialize mRequiredColumns");
			}
		}
		return new RequiredFieldResponse(true);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PigStorage)
			return equals((PigStorage) obj);
		else
			return false;
	}

	public boolean equals(MultiDelimiterStorage other) {
		return this.fieldDel == other.fieldDel;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() {
		if (loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
			return new Bzip2TextInputFormat();
		} else {
			return new PigTextInputFormat();
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) {
		in = reader;
		if (tagSource) {
			sourcePath = ((FileSplit) split.getWrappedSplit()).getPath();
		}
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		loadLocation = location;
		FileInputFormat.setInputPaths(job, location);
	}

	@SuppressWarnings("rawtypes")
	public OutputFormat getOutputFormat() {
		return new MultiDelimiterTextOutputFormat(fieldDel);
	}

	@SuppressWarnings("rawtypes")
	public void prepareToWrite(RecordWriter writer) {
		this.writer = writer;
	}

	@SuppressWarnings("unchecked")
	public void setStoreLocation(String location, Job job) throws IOException {
		job.getConfiguration().set("mapred.textoutputformat.separator", "");
		FileOutputFormat.setOutputPath(job, new Path(location));

		if ("true".equals(job.getConfiguration().get(
				"output.compression.enabled"))) {
			FileOutputFormat.setCompressOutput(job, true);
			String codec = job.getConfiguration().get(
					"output.compression.codec");
			try {
				FileOutputFormat.setOutputCompressorClass(job,
						(Class<? extends CompressionCodec>) Class
								.forName(codec));
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Class not found: " + codec);
			}
		} else {
			// This makes it so that storing to a directory ending with ".gz" or
			// ".bz2" works.
			setCompression(new Path(location), job);
		}
	}

	private void setCompression(Path path, Job job) {
		String location = path.getName();
		if (location.endsWith(".bz2") || location.endsWith(".bz")) {
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		} else if (location.endsWith(".gz")) {
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		} else {
			FileOutputFormat.setCompressOutput(job, false);
		}
	}

	public void checkSchema(ResourceSchema s) throws IOException {

	}

	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		return LoadFunc.getAbsolutePath(location, curDir);
	}

	@Override
	public int hashCode() {
		return fieldDel.hashCode();
	}

	@Override
	public void setUDFContextSignature(String signature) {
		this.signature = signature;
	}

	public List<OperatorSet> getFeatures() {
		return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
	}

	public void setStoreFuncUDFContextSignature(String signature) {
	}

	public void cleanupOnFailure(String location, Job job) throws IOException {
		StoreFunc.cleanupOnFailureImpl(location, job);
	}

	public ResourceSchema getSchema(String location, Job job)
			throws IOException {
		if (!dontLoadSchema) {
			schema = (new JsonMetadata()).getSchema(location, job, isSchemaOn);

			if (signature != null && schema != null) {
				if (tagSource) {
					schema = Utils.getSchemaWithInputSourceTag(schema);
				}
				Properties p = UDFContext.getUDFContext().getUDFProperties(
						this.getClass(), new String[] { signature });
				p.setProperty(signature + ".schema", schema.toString());
			}
		}
		return schema;
	}

	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {
		return null;
	}

	public void setPartitionFilter(Expression partitionFilter)
			throws IOException {
	}

	public String[] getPartitionKeys(String location, Job job)
			throws IOException {
		return null;
	}

	public void storeSchema(ResourceSchema schema, String location, Job job)
			throws IOException {
		if (isSchemaOn) {
			MultiDelimiterJsonMetadata metadataWriter = new MultiDelimiterJsonMetadata();
			byte recordDel = '\n';
			metadataWriter.setFieldDel(fieldDel.getBytes());
			metadataWriter.setRecordDel(recordDel);
			metadataWriter.storeSchema(schema, location, job);
		}
	}

	public void storeStatistics(ResourceStatistics stats, String location,
			Job job) throws IOException {

	}
}