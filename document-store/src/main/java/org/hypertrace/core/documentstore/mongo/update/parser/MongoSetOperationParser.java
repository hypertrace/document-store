package org.hypertrace.core.documentstore.mongo.update.parser;

import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.SET;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.merge;

import com.mongodb.BasicDBObject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;

@AllArgsConstructor
public class MongoSetOperationParser extends MongoOperationParser {
  private static final String SET_CLAUSE = "$set";

  @Override
  public UpdateOperator operator() {
    return SET;
  }

  @Override
  BasicDBObject wrapWithOperator(final List<BasicDBObject> parsed) {
    return new BasicDBObject(SET_CLAUSE, merge(parsed));
  }
}
