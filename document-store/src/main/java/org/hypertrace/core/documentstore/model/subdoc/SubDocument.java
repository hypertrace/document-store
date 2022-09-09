package org.hypertrace.core.documentstore.model.subdoc;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class SubDocument {
  String path;
}
