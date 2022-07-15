package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

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
            .max(
                (a, b) -> {
                  if (a.length() > b.length()) return 1;
                  else if (a.length() < b.length()) return -1;
                  else return 0;
                });

    if (parentField.isEmpty()) {
      return new FieldToPgColumn(orgFieldName, PostgresUtils.DOCUMENT_COLUMN);
    }

    String pgColumn = postgresQueryParser.getPgColumnNames().get(parentField.get());

    if (parentField.get().equals(orgFieldName)) {
      return new FieldToPgColumn(orgFieldName, pgColumn);
    }

    String childField = StringUtils.removeStart(orgFieldName, parentField.get() + DOT);
    return new FieldToPgColumn(childField, pgColumn);
  }
}
