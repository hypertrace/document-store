package org.hypertrace.core.documentstore.model.subdoc.visitor;

import com.mongodb.BasicDBObject;
import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;

public interface SubDocumentValueVisitor {
  Object visit(final PrimitiveSubDocumentValue value);

  BasicDBObject visit(final NestedSubDocumentValue value);
}
