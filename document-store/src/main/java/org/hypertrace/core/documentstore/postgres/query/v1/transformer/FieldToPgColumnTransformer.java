package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.Comparator;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

public class FieldToPgColumnTransformer {
  private static final String DOT = ".";

  private PostgresQueryParser postgresQueryParser;

  public FieldToPgColumnTransformer(PostgresQueryParser postgresQueryParser) {
    this.postgresQueryParser = postgresQueryParser;
  }

  public FieldToPgColumn transform(String orgFieldName) {
    Optional<String> parentField =
        postgresQueryParser.getPgColumnNames().keySet().stream()
            .filter(f -> orgFieldName.startsWith(f))
            .max(Comparator.comparingInt(String::length));

    if (parentField.isEmpty()) {
      return new FieldToPgColumn(orgFieldName, PostgresUtils.DOCUMENT_COLUMN);
    }

    String pgColumn = postgresQueryParser.getPgColumnNames().get(parentField.get());

    if (parentField.get().equals(orgFieldName)) {
      return new FieldToPgColumn(null, pgColumn);
    }

    String childField = StringUtils.removeStart(orgFieldName, parentField.get() + DOT);
    return new FieldToPgColumn(childField, pgColumn);
  }
}
