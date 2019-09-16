/*
 * Copyright 2019 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.io.bigquery.hive;

import java.io.IOException;
import java.util.Properties;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Wrapper class for BigQuery writes.
 */
public class WrappedBigQueryAvroOutputFormat extends AvroContainerOutputFormat {
    private static final Logger LOG = LoggerFactory
        .getLogger(WrappedBigQueryAvroOutputFormat.class);
    public static final String TEMP_NAME = "_hadoop_temporary_";

    @Override
    public RecordWriter getHiveRecordWriter(JobConf jobConf, Path path,
        Class<? extends Writable> valueClass, boolean isCompressed,
        Properties properties, Progressable progressable) throws IOException {

        Path actual = new Path(getTempFilename(jobConf));
        LOG.warn("Set temporary output file to {}", actual.getName());

        return super.getHiveRecordWriter(
                    jobConf, actual, valueClass, isCompressed, properties, progressable);
    }

    /*
     * Generate a temporary file name that stores the temporary Avro output from the
     * AvroContainerOutputFormat. The file will be loaded into BigQuery later.
     */
    public static String getTempFilename(JobConf jobConf) {
        String outputTableId =
            jobConf.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY);
        String tempOutputPath = jobConf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY);
        String tempDataset = jobConf.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY) + TEMP_NAME;
        String uniqueID = jobConf.get(HiveBigQueryConstants.UNIQUE_JOB_KEY);

        String tempTable = String.format("%s_%s_%s",
            tempDataset, outputTableId.replace("$","__"), uniqueID);

        return tempOutputPath + tempTable;
    }

}
