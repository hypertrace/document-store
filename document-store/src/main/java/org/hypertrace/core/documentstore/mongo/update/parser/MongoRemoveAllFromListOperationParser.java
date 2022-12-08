package org.hypertrace.core.documentstore.mongo.update.parser;

import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.REMOVE_ALL_FROM_LIST;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.merge;

import com.mongodb.BasicDBObject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;

@AllArgsConstructor
public class MongoRemoveAllFromListOperationParser extends MongoOperationParser {
  private static final String PULL_ALL_CLAUSE = "$pullAll";

  @Override
  public UpdateOperator operator() {
    return REMOVE_ALL_FROM_LIST;
  }

  @Override
  BasicDBObject wrapWithOperator(final List<BasicDBObject> parsed) {
    return new BasicDBObject(PULL_ALL_CLAUSE, merge(parsed));
  }
}
