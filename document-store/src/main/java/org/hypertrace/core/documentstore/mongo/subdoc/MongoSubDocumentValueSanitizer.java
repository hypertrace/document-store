package org.hypertrace.core.documentstore.mongo.subdoc;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.sanitizeJsonString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.BasicDBObject;
import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

public class MongoSubDocumentValueSanitizer implements SubDocumentValueVisitor {

  @SuppressWarnings("unchecked")
  @Override
  public Object visit(final PrimitiveSubDocumentValue value) {
    return value.getValue();
  }

  @SuppressWarnings("unchecked")
  @Override
  public BasicDBObject visit(final NestedSubDocumentValue value) {
    try {
      return BasicDBObject.parse(sanitizeJsonString(value.getJsonValue()));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
