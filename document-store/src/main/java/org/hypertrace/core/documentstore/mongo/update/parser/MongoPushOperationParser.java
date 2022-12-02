package org.hypertrace.core.documentstore.mongo.update.parser;

import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.APPEND_TO_LIST;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.merge;

import com.mongodb.BasicDBObject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;

@AllArgsConstructor
public class MongoPushOperationParser extends MongoOperationParser {
  private static final String PUSH_CLAUSE = "$push";

  @Override
  public UpdateOperator operator() {
    return APPEND_TO_LIST;
  }

  @Override
  BasicDBObject wrapWithOperator(final List<BasicDBObject> parsed) {
    return new BasicDBObject(PUSH_CLAUSE, merge(parsed));
  }

  @Override
  protected BasicDBObject parseUpdate(final String path, final Object value) {
    return new BasicDBObject(path, new BasicDBObject(EACH_CLAUSE, value));
  }
}
