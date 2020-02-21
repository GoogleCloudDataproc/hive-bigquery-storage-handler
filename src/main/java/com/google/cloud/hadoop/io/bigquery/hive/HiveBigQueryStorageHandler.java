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
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class {@link HiveBigQueryStorageHandler} contains the functionality to read & write data from &
 * to BigQuery tables from hive. Provides additional capabilities such as metadata validation and
 * projection/Predicate pushdown
 */
public class HiveBigQueryStorageHandler extends DefaultStorageHandler
    implements HiveStoragePredicateHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HiveBigQueryStorageHandler.class);

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
    return new HiveBigQueryMetaHook();
  }

  /**
   * Set Mapreduce Job input properties (if not provided) from Hive Table properties
   *
   * @param tableDesc Represents Hive Table description
   * @param jobProperties Represents Map Reduce Job Properties
   */
  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

    LOG.info(
        "Configuring MapReduce Job Input Properties  (if not provided) from Hive Table properties ");

    ImmutableMap<String, String> inputConfigurations =
        ImmutableMap.of(
            BigQueryConfiguration.INPUT_PROJECT_ID_KEY, BigQueryConfiguration.PROJECT_ID_KEY,
            BigQueryConfiguration.INPUT_DATASET_ID_KEY,
                HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY,
            BigQueryConfiguration.INPUT_TABLE_ID_KEY,
                HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY);

    Properties tableProperties = tableDesc.getProperties();

    // If input properties were not specified assign the default properties
    for (String config : inputConfigurations.keySet()) {
      if (Strings.isNullOrEmpty(tableProperties.getProperty(config))) {
        jobProperties.put(config, tableProperties.getProperty(inputConfigurations.get(config)));
      }
    }

    this.setJobConfProperties(jobProperties);
  }

  /**
   * Set Mapreduce Job Output properties (if not provided) from Hive Table properties
   *
   * @param tableDesc Represents Hive Table description
   * @param jobProperties Represents Map Reduce Job Properties
   */
  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

    LOG.info(
        "Configuring MapReduce Job Output Properties  (if not provided) from Hive Table properties ");

    ImmutableMap<String, String> outputConfigurations =
        ImmutableMap.of(
            BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY, BigQueryConfiguration.PROJECT_ID_KEY,
            BigQueryConfiguration.OUTPUT_DATASET_ID_KEY,
                HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY,
            BigQueryConfiguration.OUTPUT_TABLE_ID_KEY,
                HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY);

    Properties tableProperties = tableDesc.getProperties();

    // If output properties were not specified assign the default properties
    for (String config : outputConfigurations.keySet()) {
      if (Strings.isNullOrEmpty(tableProperties.getProperty(config))) {
        jobProperties.put(config, tableProperties.getProperty(outputConfigurations.get(config)));
      }
    }

    jobProperties.put(
        BigQueryConfiguration.OUTPUT_FILE_FORMAT_KEY,
        BigQueryFileFormat.AVRO.getFormatIdentifier());
    jobProperties.put(
        BigQueryConfiguration.OUTPUT_FORMAT_CLASS_KEY,
        WrappedBigQueryAvroOutputFormat.class.getCanonicalName());

    this.setJobConfProperties(jobProperties);
  }

  /**
   * Sets job properties to hive configuration
   *
   * @param jobProperties Mapreduce job properties
   */
  private void setJobConfProperties(Map<String, String> jobProperties) {

    Configuration jobConf = this.getConf();
    jobProperties
        .keySet()
        .forEach((String property) -> jobConf.set(property, jobProperties.get(property)));
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    LOG.info("Configuring MapReduce Job configuration.. ");

    ImmutableList<String> propertyNames =
        ImmutableList.of(
            BigQueryConfiguration.TEMP_GCS_PATH_KEY,
            BigQueryConfiguration.GCS_BUCKET_KEY,
            HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY,
            HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY);

    Properties tableProperties = tableDesc.getProperties();

    for (String propertyName : propertyNames) {
      jobConf.set(propertyName, tableProperties.getProperty(propertyName));
    }

    // consider move this to the OutputFormat. The reason this is here is that Hive creates
    // multiple OutputFormat instances.
    // Unique id to distinguish jobs, only use last 12 characters of UUID
    String jobID = UUID.randomUUID().toString().replace("-", "_");
    jobID = jobID.substring(jobID.length() - 12);
    jobConf.set(HiveBigQueryConstants.UNIQUE_JOB_KEY, jobID);
  }

  @Override
  public DecomposedPredicate decomposePredicate(
      JobConf jobConf, Deserializer deserializer, ExprNodeDesc predicate) {
    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
    // adding Comparison Operators
    for (String comparisionOperator :
        new String[] {
          "GenericUDFOPEqual",
          "GenericUDFOPGreaterThan",
          "GenericUDFOPLessThan",
          "GenericUDFOPEqualOrGreaterThan",
          "GenericUDFOPEqualOrLessThan",
          "GenericUDFIn",
          "GenericUDFBetween",
          "GenericUDFOPNot",
          "GenericUDFOPNull",
          "GenericUDFOPNotNull",
          "GenericUDFOPNotEqual",
          "GenericUDFOPAnd",
          "GenericUDFOPOr"
        }) {

      analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic." + comparisionOperator);
    }

    // Adding Columns that are eligible for predicate push down
    String columnNames = jobConf.get(HiveBigQueryConstants.PREDICATE_PUSHDOWN_COLUMNS, "");
    if (columnNames.length() > 0) {
      LOG.info("{} - {}", HiveBigQueryConstants.PREDICATE_PUSHDOWN_COLUMNS, columnNames);

      Arrays.stream(columnNames.split(HiveBigQueryConstants.DELIMITER))
          .forEach(columnName -> analyzer.allowColumnName(columnName));
    }

    List<IndexSearchCondition> searchConditions = new ArrayList<>();

    // filtering out residual and pushed predicate
    ExprNodeGenericFuncDesc residualPredicate =
        (ExprNodeGenericFuncDesc) analyzer.analyzePredicate(predicate, searchConditions);

    // Convert the values of timestamp and date data types to String format since these type values
    // are represented in numeric
    ImmutableList<String> typesToConvertToString = ImmutableList.of("date", "timestamp");
    searchConditions.stream()
        .filter(
            searchCondition ->
                typesToConvertToString.contains(
                    searchCondition.getColumnDesc().getTypeInfo().getTypeName().toLowerCase()))
        .forEach(
            searchCondition ->
                searchCondition
                    .getConstantDesc()
                    .setValue(
                        String.format("'%s'", searchCondition.getConstantDesc().getExprString())));

    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(searchConditions);
    decomposedPredicate.residualPredicate = residualPredicate;
    if (decomposedPredicate.pushedPredicate != null)
      LOG.info("Predicates pushed: " + decomposedPredicate.pushedPredicate.getExprString());
    if (decomposedPredicate.residualPredicate != null)
      LOG.info("Predicates not Pushed: " + decomposedPredicate.residualPredicate.getExprString());

    return decomposedPredicate;
  }
}
