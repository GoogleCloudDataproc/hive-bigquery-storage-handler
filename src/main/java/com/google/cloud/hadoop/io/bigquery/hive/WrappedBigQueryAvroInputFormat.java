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


import com.google.cloud.hadoop.io.bigquery.DirectBigQueryInputFormat;
import com.google.cloud.hadoop.io.bigquery.DirectBigQueryInputFormat.DirectBigQueryInputSplit;
import com.google.cloud.hadoop.io.bigquery.hive.util.ReflectedTaskAttemptContextFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Wrapper to allow DirectBigQueryInputFormat to be used in mapred API.
 */
public class WrappedBigQueryAvroInputFormat extends
    FileInputFormat<NullWritable, AvroGenericRecordWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(WrappedBigQueryAvroInputFormat.class);
    private org.apache.hadoop.mapreduce.InputFormat<NullWritable, GenericRecord>
            mapreduceInputFormat = new DirectBigQueryInputFormat();

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        // We don't really use any information in the job Context
        List<org.apache.hadoop.mapreduce.InputSplit> mapreduceSplits;
        try {
            mapreduceSplits = mapreduceInputFormat.getSplits(Job.getInstance(job));
        } catch (InterruptedException ex) {
            throw new IOException("Interrupted", ex);
        }
        if (mapreduceSplits == null) {
            return null;
        }

        //Wrap DirectBigQueryInputSplit inside this FileSplit
        FileSplit[] splits = new FileSplit[mapreduceSplits.size()];
        Path path = new Path(job.get("location"));
        int ii = 0;
        for (org.apache.hadoop.mapreduce.InputSplit mapreduceSplit :
            mapreduceSplits) {
            LOG.info("Split[{}] = {}", ii, mapreduceSplit);
            splits[ii++] = new BigQueryMapredInputSplit(mapreduceSplit, path);
        }
        return splits;
    }

    @Override
    public RecordReader<NullWritable, AvroGenericRecordWritable> getRecordReader(
        InputSplit inputSplit, JobConf conf, Reporter reporter) throws IOException {
        Preconditions.checkArgument(inputSplit instanceof BigQueryMapredInputSplit,
               "Split must be an instance of BigQueryMapredInputSplit");

        try {
            // The assertion is that this taskAttemptId isn't actually used, but in Hadoop2 calling
            // toString() on an emptyJobID results in an NPE.
            TaskAttemptID taskAttemptId = new TaskAttemptID();
            TaskAttemptContext context =
                    ReflectedTaskAttemptContextFactory.getContext(conf, taskAttemptId);
            org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit =
                    ((BigQueryMapredInputSplit) inputSplit).getMapreduceInputSplit();
            LOG.info("mapreduceInputSplit is {}, class is {}",
                    mapreduceInputSplit, mapreduceInputSplit.getClass().getName());
            org.apache.hadoop.mapreduce.RecordReader<NullWritable, GenericRecord>
                mapreduceRecordReader = mapreduceInputFormat.createRecordReader(
                    mapreduceInputSplit, context);
            mapreduceRecordReader.initialize(mapreduceInputSplit, context);
            long splitLength = inputSplit.getLength();

            return new BigQueryMapredAvroRecordReader(mapreduceRecordReader, splitLength);
        } catch (InterruptedException ex) {
            throw new IOException("Interrupted", ex);
        }
    }


    static class BigQueryMapredInputSplit extends HiveInputSplit {
        private org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit;
        private Writable writableInputSplit;
        private Path path;

        @VisibleForTesting
        BigQueryMapredInputSplit() {
            // Used by Hadoop serialization via reflection.
            this(new DirectBigQueryInputSplit("dummy","",0), null);
        }

        /**
         * @param mapreduceInputSplit An InputSplit that also
         *        implements Writable.
         * @param path A HCFS path of that split. Hive assumes tables are file-based.
         */
        BigQueryMapredInputSplit(
            org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit, Path path) {
            super();
            this.mapreduceInputSplit = mapreduceInputSplit;
            this.writableInputSplit = (Writable) mapreduceInputSplit;
            this.path = path;
        }

        /**
         * @param mapreduceInputSplit An InputSplit that also
         *        implements Writable.
         */
        public BigQueryMapredInputSplit(
            org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit) {
            Preconditions.checkArgument(
                mapreduceInputSplit instanceof Writable,
                "inputSplit must also be Writable");
            this.mapreduceInputSplit = mapreduceInputSplit;
            writableInputSplit = (Writable) mapreduceInputSplit;
        }

        @Override
        public long getLength() {
            return 1L;
        }

        @Override
        public String[] getLocations() throws IOException {
            try {
                return mapreduceInputSplit.getLocations();
            } catch (InterruptedException ex) {
                throw new IOException("Interrupted", ex);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            path = new Path(Text.readString(in));
            writableInputSplit.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, path.toString());
            writableInputSplit.write(out);
        }

        public org.apache.hadoop.mapreduce.InputSplit getMapreduceInputSplit() {
            return mapreduceInputSplit;
        }

        @Override
        public String toString() {
            return mapreduceInputSplit.toString();
        }

        @Override
        public Path getPath() {
            return path;
        }
    }

}
