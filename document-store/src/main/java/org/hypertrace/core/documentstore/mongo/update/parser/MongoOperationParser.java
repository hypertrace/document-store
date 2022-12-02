package org.hypertrace.core.documentstore.mongo.update.parser;

import static java.util.stream.Collectors.toUnmodifiableList;

import com.mongodb.BasicDBObject;
import java.util.List;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;
import org.hypertrace.core.documentstore.mongo.subdoc.MongoSubDocumentValueSanitizer;

public abstract class MongoOperationParser {
  protected static final String EACH_CLAUSE = "$each";

  abstract UpdateOperator operator();

  abstract BasicDBObject wrapWithOperator(final List<BasicDBObject> parsed);

  final BasicDBObject parse(final List<SubDocumentUpdate> updates) {
    final UpdateOperator operator = operator();
    final List<BasicDBObject> parsedUpdates =
        updates.stream()
            .filter(update -> operator.equals(update.getOperator()))
            .map(this::parseUpdate)
            .collect(toUnmodifiableList());
    return parsedUpdates.isEmpty() ? new BasicDBObject() : wrapWithOperator(parsedUpdates);
  }

  private BasicDBObject parseUpdate(final SubDocumentUpdate update) {
    final MongoSubDocumentValueSanitizer sanitizer = new MongoSubDocumentValueSanitizer(operator());
    final String path = update.getSubDocument().getPath();
    final Object value = update.getSubDocumentValue().accept(sanitizer);
    return parseUpdate(path, value);
  }

  protected BasicDBObject parseUpdate(final String path, final Object value) {
    return new BasicDBObject(path, value);
  }
}
