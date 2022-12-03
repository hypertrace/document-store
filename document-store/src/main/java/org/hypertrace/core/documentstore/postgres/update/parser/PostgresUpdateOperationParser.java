package org.hypertrace.core.documentstore.postgres.update.parser;

import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.postgres.Params;

public interface PostgresUpdateOperationParser {
  String parseInternal(final UpdateParserInput input);

  String parseLeaf(final UpdateParserInput input);

  @Value
  @Builder
  class UpdateParserInput {
    String baseField;
    String[] path;
    SubDocumentUpdate update;
    Params.Builder paramsBuilder;
  }
}
