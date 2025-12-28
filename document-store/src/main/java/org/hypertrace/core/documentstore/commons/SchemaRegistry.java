package org.hypertrace.core.documentstore.commons;

import java.util.Map;

public interface SchemaRegistry<T extends ColumnMetadata> {

  Map<String, T> getSchema(String tableName);

  void invalidate(String tableName);

  T getColumnOrRefresh(String tableName, String colName);
}
