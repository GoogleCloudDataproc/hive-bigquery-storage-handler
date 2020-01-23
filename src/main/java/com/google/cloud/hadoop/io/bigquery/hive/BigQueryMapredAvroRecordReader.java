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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * Class {@link BigQueryMapredAvroRecordReader} wraps  mapreduce RecordReader and can be
 * used from Hadoop streaming and Hive
 */
public class BigQueryMapredAvroRecordReader
    implements RecordReader<NullWritable, AvroGenericRecordWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMapredAvroRecordReader.class);
  private org.apache.hadoop.mapreduce.RecordReader<NullWritable, GenericRecord>   mapreduceRecordReader;
  private final long splitLength;

  /**
   * This class is just a wrapper of DirectBigQueryRecordReader
   *   for Hive to use the mapred API.
   * @param mapreduceRecordReader A mapreduce-based RecordReader.
   */
  public BigQueryMapredAvroRecordReader(
       org.apache.hadoop.mapreduce.RecordReader<NullWritable, GenericRecord> mapreduceRecordReader
      ,long splitLength) {
    this.mapreduceRecordReader = mapreduceRecordReader;
    this.splitLength = splitLength;
  }

  @Override
  public boolean next(NullWritable key, AvroGenericRecordWritable value) throws IOException {
    try {
      //splitLength check has been added to avoid stream finalize error when querying empty table.
      if (this.splitLength > 0 && this.mapreduceRecordReader.nextKeyValue()) {
        //key = this.mapreduceRecordReader.getCurrentKey();
        GenericRecord nextValue = this.mapreduceRecordReader.getCurrentValue();
        value.setFileSchema(nextValue.getSchema());
        value.setRecord(nextValue);
        return true;
      }

      return false;
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted", ex);
    }
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public AvroGenericRecordWritable createValue() {
    return new AvroGenericRecordWritable();
  }

  @Override
  public long getPos() throws IOException {
    return splitLength * (long) getProgress();
  }

  @Override
  public float getProgress() throws IOException {
    try {
      return mapreduceRecordReader.getProgress();
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted", ex);
    }
  }

  @Override
  public void close() throws IOException {
    mapreduceRecordReader.close();
  }

}