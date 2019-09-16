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

/**
 * Wrap our mapreduce RecordReader so it can be called
 * from Hadoop streaming and Hive.
 */
public class BigQueryMapredAvroRecordReader
    implements RecordReader<NullWritable, AvroGenericRecordWritable> {

  private org.apache.hadoop.mapreduce.RecordReader<NullWritable, GenericRecord>
      mapreduceRecordReader;
  private long splitLength;
  private static final Logger LOG = LoggerFactory.getLogger(
      BigQueryMapredAvroRecordReader.class);
  private Schema schema;

  /**
   * This class is just a wrapper of DirectBigQueryRecordReader
   *   for Hive to use the mapred API.
   * @param mapreduceRecordReader A mapreduce-based RecordReader.
   */
  public BigQueryMapredAvroRecordReader(
      org.apache.hadoop.mapreduce.RecordReader<NullWritable, GenericRecord>
          mapreduceRecordReader, long splitLength) {
    this.mapreduceRecordReader = mapreduceRecordReader;
    this.splitLength = splitLength;
  }

  public void close() throws IOException {
    mapreduceRecordReader.close();
  }

  @Override
  public boolean next(NullWritable key, AvroGenericRecordWritable value) throws IOException {
    try {
      boolean hasNext = mapreduceRecordReader.nextKeyValue();
      if (!hasNext) {
        return false;
      }
      NullWritable nextKey = mapreduceRecordReader.getCurrentKey();
      GenericRecord nextValue = mapreduceRecordReader.getCurrentValue();
      this.schema = nextValue.getSchema();
      key = createKey();
      value.setRecord(GenericData.get().deepCopy(this.schema, nextValue));
      value.setFileSchema(this.schema);

      return true;
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted", ex);
    }
  }

  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public AvroGenericRecordWritable createValue() {
    return new AvroGenericRecordWritable();
  }

  public long getPos() throws IOException {
    return splitLength * (long) getProgress();
  }

  public float getProgress() throws IOException {
    try {
      return mapreduceRecordReader.getProgress();
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted", ex);
    }
  }

}