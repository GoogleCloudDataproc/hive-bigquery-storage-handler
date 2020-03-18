# Hive-BigQuery StorageHandler  
  
This is a Hive StorageHandler plugin that enables Hive to interact with BigQuery. It allows you keep
your existing pipelines but move to BigQuery. It utilizes the high throughput 
[BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage/) to read data and
 uses the BigQuery API to write data.  
   
 The following steps are performed under Dataproc cluster in Google Cloud Platform. If you need to run in your cluster, you
 will need setup Google Cloud SDK and Google Cloud Storage connector for Hadoop.  
  
## Getting the StorageHandler  
  
1. Check it out from GitHub.  
2. Build it with the new Google [Hadoop BigQuery Connector](https://cloud.google.com/dataproc/docs/concepts/connectors/bigquery)   
``` shell  
git clone https://github.com/GoogleCloudPlatform/hive-bigquery-storage-handler  
cd hive-bigquery-storage-handler  
mvn clean install  
```  
3. Deploy hive-bigquery-storage-handler-1.0-shaded.jar   
  
## Using the StorageHandler to access BigQuery  
  
1. Enable the BigQuery Storage API. Follow [these instructions](https://cloud.google.com/bigquery/docs/reference/storage/#enabling_the_api)  and check [pricing details](https://cloud.google.com/bigquery/pricing#storage-api) 
       
3. Copy the compiled Jar to a Google Cloud Storage bucket that can be accessed by your hive cluster  
  
4. Open Hive CLI and load the jar as shown below:  
  
```shell  
hive> add jar gs://<Jar location>/hive-bigquery-storage-handler-1.0-shaded.jar;  
```  
  
4. Verify the jar is loaded successfully  
  
```shell  
hive> list jars;  
```  
  
At this point you can operate Hive just like you used to do.  
  
### Creating BigQuery tables  
If you have BigQuery table already, here is how you can define Hive table that refer to it:  
```sql  
CREATE TABLE bq_test (word_count bigint, word string)  
 STORED BY 
 'com.google.cloud.hadoop.io.bigquery.hive.HiveBigQueryStorageHandler' 
 TBLPROPERTIES ( 
 'bq.dataset'='<BigQuery dataset name>', 
 'bq.table'='<BigQuery table name>', 
 'mapred.bq.project.id'='<Your Project ID>', 
 'mapred.bq.temp.gcs.path'='gs://<Bucket name>/<Temporary path>', 
 'mapred.bq.gcs.bucket'='<Cloud Storage Bucket name>' 
 );
```  

You will need to provide the following table properties:  
  
| Property | Value |  
|--------|-----|  
| bq.dataset | BigQuery dataset id (Optional if hive database name matches BQ dataset name) |  
| bq.table | BigQuery table name (Optional if hive table name matches BQ table name) |  
| mapred.bq.project.id | Your project id |  
| mapred.temp.gcs.path | Temporary file location in GCS bucket |  
| mapred.bq.gcs.bucket | Temporary GCS bucket name |  
  
### Data Type Mapping  
  
| BigQuery | Hive | DESCRIPTION | 
|--------|-----| ---------------|  
| INTEGER | BIGINT | Signed 8-byte Integer  
| FLOAT | DOUBLE | 8-byte double precision floating point number  
| DATE | DATE | FORMAT IS YYYY-[M]M-[D]D. The range of values supported for the Date type is 0001-­01-­01 to 9999-­12-­31  
| TIMESTAMP | TIMESTAMP | Represents an absolute point in time since Unix epoch with millisecond precision (on Hive) compared to Microsecond precision on Bigquery.  
| BOOLEAN | BOOLEAN | Boolean values are represented by the keywords TRUE and FALSE  
| STRING | STRING | Variable-length character data  
| BYTES | BINARY | Variable-length binary data  
| REPEATED | ARRAY | Represents repeated values  
| RECORD | STRUCT | Represents nested structures  
  
### Filtering
The new API allows column pruning and predicate filtering to only read the data you are interested in.

#### Column Pruning
Since BigQuery is [backed by a columnar datastore](https://cloud.google.com/blog/big-data/2016/04/inside-capacitor-bigquerys-next-generation-columnar-storage-format), it can efficiently stream data without reading all columns.

#### Predicate Filtering
The Storage API supports arbitrary pushdown of predicate filters. To enable predicate pushdown ensure <b>hive.optimize.ppd</b> is set to true. <br/>
Filters on all primitive type columns will be pushed to storage layer improving the performance of reads. Predicate pushdown is not supported on complex types such as arrays and structs.  For example - filters like `address.city = "Sunnyvale"` will not get pushdown to Bigquery.
  
### Caveats  
1. Ensure that table exists in bigquery and column names are always lowercase  
2. timestamp column in hive is interpreted to be timezoneless and stored as an offset from the UNIX epoch with milliseconds precision.  
   To display in human readable format from_unix_time udf can be used as   
   ```sql   
   from_unixtime(cast(cast(<timestampcolumn> as bigint)/1000 as bigint), 'yyyy-MM-dd hh:mm:ss')      
   ```  
  ### Issues  
1. Writing to BigQuery will fail when using Apache Tez as the execution engine. As a workaround ```set hive.execution.engine=mr``` to use MapReduce as the execution engine
2. STRUCT type is not supported unless avro schema is explicitly specified using either avro.schema.literal or avro.schema.url table properties.
   Below table contains all supported types defining schema explicitly.  Note: If table doesn't need struct then specifying schema is optional
   ```sql
    CREATE TABLE dbname.alltypeswithSchema(currenttimestamp TIMESTAMP,currentdate DATE, userid BIGINT, sessionid STRING, skills Array<String>,
      eventduration DOUBLE, eventcount BIGINT, is_latest BOOLEAN,keyset BINARY,addresses ARRAY<STRUCT<status: STRING, street: STRING,city: STRING, state: STRING,zip: BIGINT>> )
      STORED BY 'com.google.cloud.hadoop.io.bigquery.hive.HiveBigQueryStorageHandler'
      TBLPROPERTIES (
       'bq.dataset'='bqdataset',
       'bq.table'='bqtable',
       'mapred.bq.project.id'='bqproject',
       'mapred.bq.temp.gcs.path'='gs://bucketname/prefix',
       'mapred.bq.gcs.bucket'='bucketname',
       'avro.schema.literal'='{"type":"record","name":"alltypesnonnull",
           "fields":[{"name":"currenttimestamp","type":["null",{"type":"long","logicalType":"timestamp-micros"}], "default" : null}
                    ,{"name":"currentdate","type":{"type":"int","logicalType":"date"}, "default" : -1},{"name":"userid","type":"long","doc":"User identifier.", "default" : -1}
                    ,{"name":"sessionid","type":["null","string"], "default" : null},{"name":"skills","type":["null", {"type":"array","items":"string"}], "default" : null}
                    ,{"name":"eventduration","type":["null","double"], "default" : null},{"name":"eventcount","type":["null","long"], "default" : null}
                    ,{"name":"is_latest","type":["null","boolean"], "default" : null},{"name":"keyset","type":["null","bytes"], "default" : null}
                    ,{"name":"addresses","type":["null", {"type":"array",
                       "items":{"type":"record","name":"__s_0",
                       "fields":[{"name":"status","type":"string"},{"name":"street","type":"string"},{"name":"city","type":"string"},{"name":"state","type":"string"},{"name":"zip","type":"long"}]
                       }}], "default" : null
                     }
                   ]
           }'
      );
   ```