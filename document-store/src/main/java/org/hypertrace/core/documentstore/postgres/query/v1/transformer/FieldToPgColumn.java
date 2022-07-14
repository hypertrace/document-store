package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class FieldToPgColumn {
  @Getter final String transformedField;
  @Getter final String pgColumn;
}
