package com.google.cloud.hadoop.io.bigquery.hive;

import com.google.common.base.Strings;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Class {@link HiveBigQueryMetaHook} can be used to validate and perform different actions during
 * creation and dropping of Hive Table.
 */
public class HiveBigQueryMetaHook implements HiveMetaHook {

  private static Logger LOG = LoggerFactory.getLogger(HiveBigQueryMetaHook.class);

  /**
   * Performs required validations prior to creating the table
   *
   * @param table Represents hive table object
   * @throws MetaException if table metadata violates the constraints
   */
  @Override
  public void preCreateTable(Table table) throws MetaException {

    // Check all mandatory table properties
    for (String property : HiveBigQueryConstants.MANDATORY_TABLE_PROPERTIES) {
      if (Strings.isNullOrEmpty(table.getParameters().get(property))) {
        throw new MetaException(property + " table property cannot be empty.");
      }
    }

    // Check compatibility with BigQuery features
    // TODO: accept DATE column 1 level partitioning
    if (table.getPartitionKeysSize() > 0) {
      throw new MetaException("Creation of Partition table is not supported.");
    }

    if (table.getSd().getBucketColsSize() > 0) {
      throw new MetaException("Creation of bucketed table is not supported");
    }

    if(!Strings.isNullOrEmpty(table.getSd().getLocation())) {
       throw new MetaException("Cannot create table in BigQuery with Location property.");
    }

    // Assign Hive databasename as BigQuery Dataset name if not provided by user
    if (Strings.isNullOrEmpty(
        table.getParameters().get(HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY))) {
      table
          .getParameters()
          .put(HiveBigQueryConstants.DEFAULT_BIGQUERY_DATASET_KEY, table.getDbName());
    }

    // Assign Hive table as BigQuery table name if not provided by user
    if (Strings.isNullOrEmpty(
        table.getParameters().get(HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY))) {
      table
          .getParameters()
          .put(HiveBigQueryConstants.DEFAULT_BIGQUERY_TABLE_KEY, table.getTableName());
    }

    List<FieldSchema> columnList = table.getSd().getCols();
    if(columnList != null && columnList.size() > 0) {
      //Set primitive Column names as PREDICATE_PUSHDOWN_COLUMNS
     String columnNames = columnList.stream()
                                    .filter(column -> HiveBigQueryConstants.PREDICATE_PUSHDOWN_ALLOWED_TYPES.contains(column.getType()))
                                    .map(column -> column.getName())
                                    .collect(Collectors.joining(HiveBigQueryConstants.DELIMITER));

      table.getParameters().put(HiveBigQueryConstants.PREDICATE_PUSHDOWN_COLUMNS,columnNames);
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // Do nothing by default
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // Do nothing by default
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // Do nothing by default
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // Do nothing by default
  }

  @Override
  public void commitDropTable(Table table, boolean b) throws MetaException {
    // Do nothing by default
  }
}
