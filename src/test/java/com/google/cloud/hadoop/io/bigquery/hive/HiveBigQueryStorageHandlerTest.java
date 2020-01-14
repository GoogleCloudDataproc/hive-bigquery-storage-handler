package com.google.cloud.hadoop.io.bigquery.hive;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.JobConf;
import org.junit.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;

/**
 *  Class contains various tests to validate HiveBigQueryStorageHandler functionality
 */
public class HiveBigQueryStorageHandlerTest {

    private static HiveBigQueryStorageHandler hiveBigQueryStorageHandler;
    private static  ImmutableMap<String,String> inputConfigurations;

    @BeforeClass
    public static void initialize(){
        hiveBigQueryStorageHandler = new HiveBigQueryStorageHandler();
        inputConfigurations = ImmutableMap.of(
                BigQueryConfiguration.INPUT_PROJECT_ID_KEY ,  BigQueryConfiguration.PROJECT_ID_KEY,
                BigQueryConfiguration.INPUT_DATASET_ID_KEY ,  HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY,
                BigQueryConfiguration.INPUT_TABLE_ID_KEY ,  HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY
        );
        hiveBigQueryStorageHandler.setConf(new Configuration());
    }

    private TableDesc createTableDescription(){
        TableDesc tableDesc = new TableDesc();
        Properties tableProperties = new Properties();
        //Assigning all the input configurations
        for(String config : inputConfigurations.keySet()) {
            tableProperties.put(config,"DummyValue");
        }
        tableProperties.put(BigQueryConfiguration.PROJECT_ID_KEY, "ReferenceProject");
        tableProperties.put(HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY, "ReferenceDataset");
        tableProperties.put(HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY, "ReferenceTable");
        tableProperties.put(BigQueryConfiguration.TEMP_GCS_PATH_KEY, "gs://hivebqstoragehandler/tmp");
        tableProperties.put(BigQueryConfiguration.GCS_BUCKET_KEY, "hivebqstoragehandler");
        tableDesc.setProperties(tableProperties);
        return tableDesc;
    }

    @Test
    public void testConfigureInputJobPropertiesWithoutDefaultValues() {
        Map<String,String> jobProperties = new HashMap<>();
        hiveBigQueryStorageHandler.configureInputJobProperties(createTableDescription(),jobProperties);
        assertEquals(0, jobProperties.size());
    }

    @Test
    public void testConfigureInputJobPropertiesWithDefaultValues() {
        Map<String,String> jobProperties = new HashMap<>();
        TableDesc tableDesc = createTableDescription();
        tableDesc.getProperties().remove(BigQueryConfiguration.INPUT_DATASET_ID_KEY);
        hiveBigQueryStorageHandler.configureInputJobProperties(tableDesc,jobProperties);
        assertThat(jobProperties, hasKey(BigQueryConfiguration.INPUT_DATASET_ID_KEY));
        assertThat(jobProperties, hasValue("ReferenceDataset"));
    }



    @Test
    public void testInputConfigureJobConf() {
        JobConf jobConf = new JobConf();
        TableDesc tableDesc = createTableDescription();
        tableDesc.getProperties().remove(BigQueryConfiguration.INPUT_PROJECT_ID_KEY);
        tableDesc.getProperties().remove(BigQueryConfiguration.INPUT_DATASET_ID_KEY);
        tableDesc.getProperties().remove(BigQueryConfiguration.INPUT_TABLE_ID_KEY);
        hiveBigQueryStorageHandler.configureInputJobProperties(tableDesc,new HashMap<>());
        hiveBigQueryStorageHandler.configureJobConf(tableDesc,jobConf);
        assertThat(jobConf.get(HiveBigQueryConstants.UNIQUE_JOB_KEY), notNullValue());
        assertThat(jobConf.get(HiveBigQueryConstants.UNIQUE_JOB_KEY).length(),is(12));
    }

    @Test
    public void testInputOutputConfigureJobConf() {
        JobConf jobConf = new JobConf();
        TableDesc tableDesc = createTableDescription();
        tableDesc.getProperties().remove(BigQueryConfiguration.INPUT_PROJECT_ID_KEY);
        tableDesc.getProperties().remove(BigQueryConfiguration.INPUT_DATASET_ID_KEY);
        tableDesc.getProperties().remove(BigQueryConfiguration.INPUT_TABLE_ID_KEY);
        hiveBigQueryStorageHandler.configureInputJobProperties(tableDesc,new HashMap<>());
        tableDesc.getProperties().setProperty(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY,"OutputDataset");
        hiveBigQueryStorageHandler.configureOutputJobProperties(tableDesc,new HashMap<>());
        hiveBigQueryStorageHandler.configureJobConf(tableDesc,jobConf);
        assertThat(jobConf.get(HiveBigQueryConstants.UNIQUE_JOB_KEY), notNullValue());
        assertThat(jobConf.get(HiveBigQueryConstants.UNIQUE_JOB_KEY).length(),is(12));
    }

    @Test
    public void testInputOutputConfigureWithSameJobPropertiesJobConf() {
        JobConf jobConf = new JobConf();
        TableDesc tableDesc = createTableDescription();
        tableDesc.getProperties().remove(BigQueryConfiguration.INPUT_PROJECT_ID_KEY);
        tableDesc.getProperties().remove(BigQueryConfiguration.INPUT_DATASET_ID_KEY);
        tableDesc.getProperties().remove(BigQueryConfiguration.INPUT_TABLE_ID_KEY);
        Map<String,String> jobProperties = new HashMap<>();
        hiveBigQueryStorageHandler.configureInputJobProperties(tableDesc,jobProperties);
        tableDesc.getProperties().setProperty(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY,"OutputDataset");
        hiveBigQueryStorageHandler.configureOutputJobProperties(tableDesc,jobProperties);
        hiveBigQueryStorageHandler.configureJobConf(tableDesc,jobConf);
        assertThat(jobConf.get(HiveBigQueryConstants.UNIQUE_JOB_KEY), notNullValue());
        assertThat(jobConf.get(HiveBigQueryConstants.UNIQUE_JOB_KEY).length(),is(12));
    }


}
