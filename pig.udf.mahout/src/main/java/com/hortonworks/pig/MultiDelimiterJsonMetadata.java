package com.hortonworks.pig;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.builtin.JsonMetadata;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class MultiDelimiterJsonMetadata extends JsonMetadata {
	
	private static final Log log = LogFactory.getLog(MultiDelimiterJsonMetadata.class);

	private String schemaFileName = ".pig_schema";
    private String headerFileName = ".pig_header";
    private boolean printHeaders = true;
	private byte[] fieldDel;
	private byte recordDel;

	public void setFieldDel(byte[] fieldDel) {
		this.fieldDel = fieldDel;
	}
	
	@Override
	public void setRecordDel(byte recordDel) {
        this.recordDel = recordDel;
    }
	
	@Override
    public void storeSchema(ResourceSchema schema, String location, Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        DataStorage storage = new HDataStorage(ConfigurationUtil.toProperties(conf));
        ElementDescriptor schemaFilePath = storage.asElement(location, schemaFileName);
        if(!schemaFilePath.exists() && schema != null) {
            try {
                new ObjectMapper().writeValue(schemaFilePath.create(), schema);
            } catch (JsonGenerationException e) {
                log.warn("Unable to write Resource Statistics for "+location);
                e.printStackTrace();
            } catch (JsonMappingException e) {
                log.warn("Unable to write Resource Statistics for "+location);
                e.printStackTrace();
            }
        }
        if (printHeaders) {
            ElementDescriptor headerFilePath = storage.asElement(location, headerFileName);
            if (!headerFilePath.exists()) {
                OutputStream os = headerFilePath.create();
                try {
                    String[] names = schema.fieldNames();

                    for (int i=0; i < names.length; i++) {
                        os.write(names[i].getBytes("UTF-8"));
                        if (i <names.length-1) {
                            os.write(fieldDel);
                        } else {
                            os.write(recordDel);
                        }
                    }
                } finally {
                    os.close();
                }
            }
        }
    }
}
