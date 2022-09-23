package org.hypertrace.core.documentstore.postgres.model;

import lombok.Value;
import org.hypertrace.core.documentstore.Document;

@Value
public class DocumentAndId {
  Document document;
  String id;
}
