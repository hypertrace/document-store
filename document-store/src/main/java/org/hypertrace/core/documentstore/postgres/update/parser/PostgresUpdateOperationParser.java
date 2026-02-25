package org.hypertrace.core.documentstore.postgres.update.parser;

import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;

public interface PostgresUpdateOperationParser {

  /**
   * Parses an update operation for a top-level column in flat collections.
   *
   * <p>For example, for SET on a top-level "price" column: {@code "price" = ?}
   *
   * @param input the update parser input containing column info and value
   * @return SQL fragment for the SET clause
   */
  default String parseTopLevelField(final UpdateParserInput input) {
    throw new UnsupportedOperationException(
        "parseTopLevelField not implemented for this operator");
  }

  String parseInternal(final UpdateParserInput input);

  String parseLeaf(final UpdateParserInput input);

  @Value
  @Builder
  class UpdateParserInput {
    String baseField;
    String[] path;
    SubDocumentUpdate update;
    Params.Builder paramsBuilder;

    /** The PostgreSQL data type of the column (used for flat collections with typed columns) */
    PostgresDataType columnType;
  }
}
