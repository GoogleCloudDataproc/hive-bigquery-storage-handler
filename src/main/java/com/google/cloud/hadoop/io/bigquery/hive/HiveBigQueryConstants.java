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
import com.google.common.collect.ImmutableList;

/** Contains all Constants */
public class HiveBigQueryConstants {
  public static final String UNIQUE_JOB_KEY = "mapred.bq.unique.job.id";
  public static final String DEFAULT_BIGQUERY_DATASET_KEY = "bq.dataset";
  public static final String DEFAULT_BIGQUERY_TABLE_KEY = "bq.table";

  public static final ImmutableList<String> MANDATORY_TABLE_PROPERTIES =
      ImmutableList.of(
          BigQueryConfiguration.PROJECT_ID_KEY,
          BigQueryConfiguration.GCS_BUCKET_KEY,
          BigQueryConfiguration.TEMP_GCS_PATH_KEY);
}
