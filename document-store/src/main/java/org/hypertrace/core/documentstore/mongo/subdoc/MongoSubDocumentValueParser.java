package org.hypertrace.core.documentstore.mongo.subdoc;

import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.ADD_TO_LIST_IF_ABSENT;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.APPEND_TO_LIST;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.REMOVE_ALL_FROM_LIST;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.sanitizeJsonString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.BasicDBObject;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.MultiValuedPrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NullSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

@AllArgsConstructor
public class MongoSubDocumentValueParser implements SubDocumentValueVisitor<Object> {
  private static final Set<UpdateOperator> ARRAY_OPERATORS =
      Set.of(ADD_TO_LIST_IF_ABSENT, REMOVE_ALL_FROM_LIST, APPEND_TO_LIST);

  private final UpdateOperator operator;

  @Override
  public Object visit(final PrimitiveSubDocumentValue value) {
    final Object primitiveValue = value.getValue();
    return ARRAY_OPERATORS.contains(operator) ? new Object[] {primitiveValue} : primitiveValue;
  }

  @Override
  public Object visit(final MultiValuedPrimitiveSubDocumentValue value) {
    return value.getValues();
  }

  @Override
  public Object visit(final NestedSubDocumentValue value) {
    try {
      return BasicDBObject.parse(sanitizeJsonString(value.getJsonValue()));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object visit(final NullSubDocumentValue value) {
    return "";
  }
}
