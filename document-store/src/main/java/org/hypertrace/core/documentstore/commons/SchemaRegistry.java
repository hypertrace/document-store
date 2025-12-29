package org.hypertrace.core.documentstore.commons;

import java.util.Map;
import java.util.Optional;

public interface SchemaRegistry<T extends ColumnMetadata> {

  Map<String, T> getSchema(String tableName);

  void invalidate(String tableName);

  Optional<T> getColumnOrRefresh(String tableName, String colName);
}
