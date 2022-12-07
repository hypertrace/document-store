package org.hypertrace.core.documentstore.mongo.update.parser;

import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.ADD_TO_LIST_IF_ABSENT;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.merge;

import com.mongodb.BasicDBObject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;

@AllArgsConstructor
public class MongoAddToListIfAbsentOperationParser extends MongoOperationParser {
  private static final String ADD_TO_SET_CLAUSE = "$addToSet";

  @Override
  public UpdateOperator operator() {
    return ADD_TO_LIST_IF_ABSENT;
  }

  @Override
  BasicDBObject wrapWithOperator(final List<BasicDBObject> parsed) {
    return new BasicDBObject(ADD_TO_SET_CLAUSE, merge(parsed));
  }

  @Override
  protected BasicDBObject parseUpdate(final String path, final Object value) {
    return new BasicDBObject(path, new BasicDBObject(EACH_CLAUSE, value));
  }
}
