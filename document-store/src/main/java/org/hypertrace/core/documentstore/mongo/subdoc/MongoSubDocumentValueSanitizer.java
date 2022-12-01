package org.hypertrace.core.documentstore.mongo.subdoc;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.sanitizeJsonString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.BasicDBObject;
import java.util.List;
import org.hypertrace.core.documentstore.model.subdoc.ArraySubDocumentValue;
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

  @SuppressWarnings("unchecked")
  @Override
  public BasicDBObject visit(final NestedSubDocumentValue value) {
    return parseDocument(value.getJsonValue());
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final NullSubDocumentValue value) {
    return "";
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<BasicDBObject> visit(final ArraySubDocumentValue value) {
    return value.getJsonValues().stream().map(this::parseDocument).collect(toUnmodifiableList());
  }

  private BasicDBObject parseDocument(final String jsonValue) {
    try {
      return BasicDBObject.parse(sanitizeJsonString(jsonValue));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
