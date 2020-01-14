package com.google.cloud.hadoop.io.bigquery.hive;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *  Class contains various tests to validate WrappedBigQueryAvroOutputFormat functionality
 */
public class WrappedBigQueryAvroOutputFormatTest {

    @Test
    public void testgetTempFilename() {
        JobConf jobConf = new JobConf();
        jobConf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, "gs://hivebqstoragehandler/staging");
        jobConf.set(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY, "mydataset");
        jobConf.set(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY, "mytable");
        jobConf.set(HiveBigQueryConstants.UNIQUE_JOB_KEY, "j78654");
        assertEquals("gs://hivebqstoragehandler/staging/mydataset_mytable_j78654",WrappedBigQueryAvroOutputFormat.getTempFilename(jobConf));
    }

}
