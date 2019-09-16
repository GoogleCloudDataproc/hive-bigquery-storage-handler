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

import com.google.cloud.hadoop.repackaged.bigquery.com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.repackaged.bigquery.com.google.cloud.hadoop.util.ConfigurationUtil;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFactory;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveBigQueryOutputCommitter extends OutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveBigQueryOutputCommitter.class);

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    JobConf conf = jobContext.getJobConf();
    String outputFileUri = WrappedBigQueryAvroOutputFormat.getTempFilename(conf);

    try {
      BigQueryOutputConfiguration.configureWithAutoSchema(
          conf,
          conf.get(
              BigQueryConfiguration.OUTPUT_DATASET_ID_KEY) + "." +
              conf.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY),
          outputFileUri,
          BigQueryFileFormat.AVRO,
          // Unused
          TextOutputFormat.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    JobConf conf = context.getJobConf();
    BigQueryFactory factory = new BigQueryFactory();

    try {
      BigQueryHelper bigquery = factory.getBigQueryHelper(conf);
      String outputFileUri = WrappedBigQueryAvroOutputFormat.getTempFilename(conf);
      List<String> loadFiles = getLoadFiles(outputFileUri, conf);
      String projectId = BigQueryOutputConfiguration.getProjectId(conf);
      if(loadFiles.size() > 0) {
        LOG.info("Ready to load temporary files under:" + outputFileUri);
        bigquery.importFromGcs(
                projectId,
                getTableReference(conf),
                null,
                null,
                null,
                BigQueryFileFormat.AVRO,
                BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION_DEFAULT,
                BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_DEFAULT,
                loadFiles,
                true);
      } else {
        LOG.error("Found no valid files under: " + outputFileUri);
      }
      removeJobOutput(conf);
    } catch (GeneralSecurityException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void abortJob(JobContext context, int runState) throws IOException {
    LOG.info("Abort BigQuery loading job");
    removeJobOutput(context.getJobConf());
    super.abortJob(context, runState);
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
    //Do nothing by default
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
    //Do nothing by default
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    LOG.info("Abort BigQuery loading task");
    JobConf conf = taskAttemptContext.getJobConf();
    removeJobOutput(conf);
  }

  // TODO: make public in BigQueryOutputConfiguration
  static TableReference getTableReference(Configuration conf) throws IOException {
    String projectId = BigQueryOutputConfiguration.getProjectId(conf);
    String datasetId = ConfigurationUtil.getMandatoryConfig(
        conf, BigQueryConfiguration.OUTPUT_DATASET_ID_KEY);
    String tableId = ConfigurationUtil.getMandatoryConfig(
        conf, BigQueryConfiguration.OUTPUT_TABLE_ID_KEY);

    return (new TableReference())
        .setProjectId(projectId)
        .setDatasetId(datasetId)
        .setTableId(tableId);
  }

  private static void removeJobOutput(JobConf jobConf) throws IOException{
    String outputFileUri = WrappedBigQueryAvroOutputFormat.getTempFilename(jobConf);
    Path outputFilePath = new Path(outputFileUri);
    FileSystem fs = outputFilePath.getFileSystem(jobConf);
    LOG.info("Removing job output from path: " + outputFileUri);
    if(fs.exists(outputFilePath))
      fs.deleteOnExit(outputFilePath);
  }

  /*
   * Get list of files to be imported into BigQuery.
   * We skip _temporary and _SUCCESS files and directories
   */
  private static List<String> getLoadFiles(String outputFileUri, JobConf jobConf)
      throws IOException{
    Path path = new Path(outputFileUri);
    FileSystem fs = path.getFileSystem(jobConf);
    ArrayList<String> loadFiles = new ArrayList<>();

    FileStatus[] fileStatuses = fs.listStatus(path);
    for(FileStatus fileStatus : fileStatuses) {
      if(fileStatus.getLen() <1)
        continue;
      if(fileStatus.getPath().getName().endsWith(FileOutputCommitter.TEMP_DIR_NAME))
        continue;
      if(fileStatus.getPath().getName().endsWith(FileOutputCommitter.SUCCEEDED_FILE_NAME))
        continue;

      loadFiles.add(fileStatus.getPath().toString());
      LOG.info("Will commit output files: " + fileStatus.toString());
    }

    return loadFiles;
  }
}
