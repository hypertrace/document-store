package org.hypertrace.core.documentstore.postgres.query.v1.mapper;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

public class FieldNameToColumnMapper {

  @AllArgsConstructor
  public static class PgFieldColumn {
    @Getter final String fieldName;
    @Getter final String columnName;
  }

  public static PgFieldColumn toColumnName(
      PostgresQueryParser postgresQueryParser, String fieldName) {
    Optional<String> foundParent =
        postgresQueryParser.getPgColumnNames().keySet().stream()
            .filter(f -> fieldName.startsWith(f))
            .max(
                (a, b) -> {
                  if (a.length() > b.length()) return 1;
                  else if (a.length() < b.length()) return -1;
                  else return 0;
                });

    if (foundParent.isEmpty()) {
      return new PgFieldColumn(fieldName, PostgresUtils.DOCUMENT_COLUMN);
    }

    String parent = foundParent.get();
    String columnName = postgresQueryParser.getPgColumnNames().get(foundParent.get());

    if (parent.equals(fieldName)) {
      return new PgFieldColumn(fieldName, columnName);
    }

    String childFieldName = fieldName.replace(parent + ".", "");
    return new PgFieldColumn(childFieldName, columnName);
  }
}
