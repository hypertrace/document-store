package org.hypertrace.core.documentstore.mongo.subdoc;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.sanitizeJsonString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.BasicDBObject;
import java.util.Arrays;
import org.hypertrace.core.documentstore.model.subdoc.MultiValuedNestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.MultiValuedPrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NullSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

public class MongoSubDocumentValueSanitizer implements SubDocumentValueVisitor {

  @SuppressWarnings("unchecked")
  @Override
  public Object visit(final PrimitiveSubDocumentValue value) {
    return value.getValue();
  }

  @Override
  public Object[] visit(final MultiValuedPrimitiveSubDocumentValue value) {
    return value.getValues();
  }

  @SuppressWarnings("unchecked")
  @Override
  public BasicDBObject visit(final NestedSubDocumentValue value) {
    return parseDocument(value.getJsonValue());
  }

  @SuppressWarnings("unchecked")
  @Override
  public BasicDBObject[] visit(final MultiValuedNestedSubDocumentValue value) {
    return Arrays.stream(value.getJsonValues())
        .map(this::parseDocument)
        .toArray(BasicDBObject[]::new);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final NullSubDocumentValue value) {
    return "";
  }

  private BasicDBObject parseDocument(final String jsonValue) {
    try {
      return BasicDBObject.parse(sanitizeJsonString(jsonValue));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
