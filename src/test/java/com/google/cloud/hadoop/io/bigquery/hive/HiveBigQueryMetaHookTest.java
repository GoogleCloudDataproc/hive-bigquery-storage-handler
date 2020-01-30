package com.google.cloud.hadoop.io.bigquery.hive;


import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


/**
 *  Class contains various tests to validate HiveBigQueryMetaHook functionality
 */

public class HiveBigQueryMetaHookTest {

    private static HiveBigQueryMetaHook hiveBigQueryMetaHook;
    private Table hiveTable;
    private Map<String,String> tableParameters;

    @BeforeClass
    public static void initialize(){
        hiveBigQueryMetaHook = new HiveBigQueryMetaHook();
    }

    @Before
    public void  setUp(){
        this.hiveTable = new Table();
        this.hiveTable.setDbName("TestDB");
        this.hiveTable.setTableName("Sample");
        this.hiveTable.setTableType("MANAGED_TABLE");

        this.tableParameters = new HashMap<>();
        this.tableParameters.put(HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY, "analytics");
        this.tableParameters.put(HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY, "metrics");
        this.tableParameters.put(BigQueryConfiguration.PROJECT_ID_KEY, "financestats");
        this.tableParameters.put(BigQueryConfiguration.GCS_BUCKET_KEY, "bucketname");
        this.tableParameters.put(BigQueryConfiguration.TEMP_GCS_PATH_KEY, "gs://bucketname/temp/");
        this.hiveTable.setParameters(this.tableParameters);

        this.hiveTable.setSd(new StorageDescriptor());

    }

    @Test(expected = MetaException.class)
    public void testMissingTablePropertiesThrowsException() throws  MetaException {
        this.hiveTable.getParameters().remove(BigQueryConfiguration.GCS_BUCKET_KEY);
        hiveBigQueryMetaHook.preCreateTable(this.hiveTable);
    }

    @Test(expected = MetaException.class)
    public void testPartitionTableThrowsException() throws  MetaException {
        this.hiveTable.setPartitionKeys(Arrays.asList(new FieldSchema()));
        hiveBigQueryMetaHook.preCreateTable(this.hiveTable);
    }

    @Test(expected = MetaException.class)
    public void testBucketColumsThrowsException() throws  MetaException {
        StorageDescriptor storageDescriptor = this.hiveTable.getSd();
        storageDescriptor.addToBucketCols("column1");
        hiveBigQueryMetaHook.preCreateTable(this.hiveTable);
    }

    @Test(expected = MetaException.class)
    public void testLocationThrowsException() throws  MetaException {
        StorageDescriptor storageDescriptor = this.hiveTable.getSd();
        storageDescriptor.setLocation("gs://bucketname/temp/");
        hiveBigQueryMetaHook.preCreateTable(this.hiveTable);
    }

    @Ignore
    @Test(expected = MetaException.class)
    public void testGcsPathFormat() throws  MetaException {
        this.tableParameters.put(BigQueryConfiguration.TEMP_GCS_PATH_KEY, "hdfs://bucketname/temp/");
        hiveBigQueryMetaHook.preCreateTable(this.hiveTable);
    }

    @Test
    public void testAssigningHiveDatabaseTableName() throws  MetaException {
        this.hiveTable.getParameters().remove(HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY);
        this.hiveTable.getParameters().remove(HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY);
        hiveBigQueryMetaHook.preCreateTable(this.hiveTable);
        assertEquals(this.hiveTable.getDbName(), this.hiveTable.getParameters().get(HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY));
        assertEquals(this.hiveTable.getTableName(), this.hiveTable.getParameters().get(HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY));
    }

}
