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

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

public class HiveBigQueryStorageHandler  extends DefaultStorageHandler implements HiveMetaHook{
    private static final Logger LOG = LoggerFactory.getLogger(HiveBigQueryStorageHandler.class);
    private static final ImmutableList<String> MANDATORY_TABLE_PROPERTIES =
        ImmutableList.of(
            HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY,
            HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY,
            BigQueryConfiguration.PROJECT_ID_KEY,
            BigQueryConfiguration.GCS_BUCKET_KEY,
            BigQueryConfiguration.TEMP_GCS_PATH_KEY);

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return WrappedBigQueryAvroInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return WrappedBigQueryAvroOutputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return AvroSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return this;
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException{
        return new DefaultHiveAuthorizationProvider();
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        //Set input job keys
        if(Strings.isNullOrEmpty(
            tableDesc.getProperties().getProperty(BigQueryConfiguration.INPUT_PROJECT_ID_KEY))) {
            jobProperties.put(BigQueryConfiguration.INPUT_PROJECT_ID_KEY,
                tableDesc.getProperties().getProperty(BigQueryConfiguration.PROJECT_ID_KEY));
        }
        if(Strings.isNullOrEmpty(
            tableDesc.getProperties().getProperty(BigQueryConfiguration.INPUT_DATASET_ID_KEY))) {
            jobProperties.put(BigQueryConfiguration.INPUT_DATASET_ID_KEY,
                tableDesc.getProperties().getProperty(HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY));
        }
        if(Strings.isNullOrEmpty(
            tableDesc.getProperties().getProperty(BigQueryConfiguration.INPUT_TABLE_ID_KEY))) {
            jobProperties.put(BigQueryConfiguration.INPUT_TABLE_ID_KEY,
                tableDesc.getProperties().getProperty(HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY));
        }
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        //Setup output job keys
        if(Strings.isNullOrEmpty(
            tableDesc.getProperties().getProperty(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY))) {
            jobProperties.put(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY,
                tableDesc.getProperties().getProperty(BigQueryConfiguration.PROJECT_ID_KEY));
        }
        if(Strings.isNullOrEmpty(
            tableDesc.getProperties().getProperty(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY))) {
            jobProperties.put(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY,
                tableDesc.getProperties().getProperty(HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY));
        }
        if(Strings.isNullOrEmpty(
            tableDesc.getProperties().getProperty(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY))) {
            jobProperties.put(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY,
                tableDesc.getProperties().getProperty(HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY));
        }
        jobProperties.put(BigQueryConfiguration.TEMP_GCS_PATH_KEY,
            tableDesc.getProperties().getProperty(BigQueryConfiguration.TEMP_GCS_PATH_KEY));
        jobProperties.put(BigQueryConfiguration.GCS_BUCKET_KEY,
            tableDesc.getProperties().getProperty(BigQueryConfiguration.GCS_BUCKET_KEY));

        jobProperties.put(BigQueryConfiguration.OUTPUT_FILE_FORMAT_KEY,
            BigQueryFileFormat.AVRO.getFormatIdentifier());
        jobProperties.put(BigQueryConfiguration.OUTPUT_FORMAT_CLASS_KEY,
            WrappedBigQueryAvroOutputFormat.class.getCanonicalName());
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        //Do nothing by default
    }

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
        tableDesc.getProperties().forEach((k, v) -> LOG.debug("{} -> {}", k, v));
        jobConf.setOutputCommitter(HiveBigQueryOutputCommitter.class);

        //consider move this to the OutputFormat. The reason this is here is that Hive creates
        // multiple OutputFormat instances.
        //Unique id to distinguish jobs, only use last 12 characters of UUID
        String jobID = UUID.randomUUID().toString().replace("-","_");
        jobID = jobID.substring(jobID.length()-12);
        jobConf.set(HiveBigQueryConstants.UNIQUE_JOB_KEY, jobID);
    }

    @Override
    public void preCreateTable(Table table) throws MetaException {
        if(table != null) {
            //Check compatibility with BigQuery features
            //TODO: accept DATE column 1 level partitioning
            if(table.getPartitionKeysSize() > 0) {
                throw new MetaException("Creating partitioned table in BigQuery is not implemented.");
            }
            if(table.getSd().getBucketColsSize() > 0) {
                throw new MetaException("Creating bucketed table in BigQuery is not implemented.");
            }
            if(!Strings.isNullOrEmpty(table.getSd().getLocation())) {
                throw new MetaException("Cannot create table in BigQuery with Location property.");
            }

            //Check all mandatory table properties
            for(String property : MANDATORY_TABLE_PROPERTIES){
                if(Strings.isNullOrEmpty(table.getParameters().get(property))) {
                    throw new MetaException(property + " table property cannot be empty.");
                }
            }
        }
    }

    @Override
    public void rollbackCreateTable(Table table) throws MetaException {
        //Do nothing by default
    }

    @Override
    public void commitCreateTable(Table table) throws MetaException {
        //Do nothing by default
    }

    @Override
    public void preDropTable(Table table) throws MetaException {
        //Do nothing by default
    }

    @Override
    public void rollbackDropTable(Table table) throws MetaException {
        //Do nothing by default
    }

    @Override
    public void commitDropTable(Table table, boolean b) throws MetaException {
        //Do nothing by default
    }
}
