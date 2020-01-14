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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Class {@link WrappedBigQueryAvroOutputFormat} serializes hive records as Avro records and writes to file on GCS
 */

public class WrappedBigQueryAvroOutputFormat extends AvroContainerOutputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(WrappedBigQueryAvroOutputFormat.class);
    private static final String TEMP_NAME = "_hadoop_temporary_";

    /**
     *  Create Hive Record writer.
     *
     * @param jobConf Hadoop Job Configuration
     * @param path  Hadoop File Path
     * @param valueClass Class representing record (in this case AvroGenericRecordWritable)
     * @param isCompressed Indicates whether records are compressed or not
     * @param properties  Job Properties
     * @param progressable Instance to represent the task progress
     * @return  Instance of Hive Record Writer
     * @throws IOException
     */
    @Override
    public RecordWriter getHiveRecordWriter(JobConf jobConf, Path path,
        Class<? extends Writable> valueClass, boolean isCompressed,
        Properties properties, Progressable progressable) throws IOException {

        Path actual = new Path(getTempFilename(jobConf));
        LOG.info("Set temporary output file to {}", actual.getName());

        return super.getHiveRecordWriter(jobConf, actual, valueClass, isCompressed, properties, progressable);
    }

    /**
     *  Checks output Spec ansd sets HiveBigQueryOutputCommitter
     *
     * @param ignored
     * @param jobConf
     * @throws IOException
     */
    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf jobConf) throws IOException {
        //can perform various checks
        LOG.info("Setting HiveBigQueryOutputCommitter..");
        jobConf.setOutputCommitter(HiveBigQueryOutputCommitter.class);
    }

    /**
     *  Generate a temporary file name that stores the temporary Avro output from the
     *  AvroContainerOutputFormat. The file will be loaded into BigQuery later.
     *
     * @param jobConf  Hadoop Job Configuration
     * @return Fully Qualified temporary table path on GCS
     */
    public static String getTempFilename(JobConf jobConf) {

        String tempOutputPath = jobConf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY);
        String tempDataset = jobConf.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY);
        String outputTableId = jobConf.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY);
        String uniqueID = jobConf.get(HiveBigQueryConstants.UNIQUE_JOB_KEY);

        Path tempFilePath = new Path(tempOutputPath, String.format("%s_%s_%s",
                                     tempDataset, outputTableId.replace("$","__"), uniqueID));

        return tempFilePath.toString();
    }

}
