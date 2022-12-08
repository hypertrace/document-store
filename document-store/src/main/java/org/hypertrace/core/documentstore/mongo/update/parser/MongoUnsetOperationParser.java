package org.hypertrace.core.documentstore.mongo.update.parser;

import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.UNSET;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.merge;

import com.mongodb.BasicDBObject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;

@AllArgsConstructor
public class MongoUnsetOperationParser extends MongoOperationParser {
  private static final String UNSET_CLAUSE = "$unset";

  @Override
  public UpdateOperator operator() {
    return UNSET;
  }

  @Override
  BasicDBObject wrapWithOperator(final List<BasicDBObject> parsed) {
    return new BasicDBObject(UNSET_CLAUSE, merge(parsed));
  }
}
