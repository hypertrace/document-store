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
    // Check if field is a first-class typed column - support both old and new patterns
    boolean isFirstClassField = false;

    // NEW: Registry-based check (preferred)
    if (postgresQueryParser.getColumnRegistry() != null
        && postgresQueryParser.getColumnRegistry().isFirstClassColumn(orgFieldName)) {
      isFirstClassField = true;
    }

    // OLD: flatStructureCollection check (for backward compatibility)
    String flatStructureCollection = postgresQueryParser.getFlatStructureCollectionName();
    if (flatStructureCollection != null
        && flatStructureCollection.equals(
            postgresQueryParser.getTableIdentifier().getTableName())) {
      isFirstClassField = true;
    }

    if (isFirstClassField) {
      return new FieldToPgColumn(null, PostgresUtils.wrapFieldNamesWithDoubleQuotes(orgFieldName));
    }
    Optional<String> parentField =
        postgresQueryParser.getPgColumnNames().keySet().stream()
            .filter(orgFieldName::startsWith)
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
