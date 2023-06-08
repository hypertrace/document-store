package org.hypertrace.core.documentstore.mongo.update.parser;

import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.INCREMENT;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.merge;

import com.mongodb.BasicDBObject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;

@AllArgsConstructor
public class MongoIncrementOperationParser extends MongoOperationParser {
  private static final String INCREMENT_CLAUSE = "$inc";

  @Override
  UpdateOperator operator() {
    return INCREMENT;
  }

  @Override
  BasicDBObject wrapWithOperator(List<BasicDBObject> parsed) {
    return new BasicDBObject(INCREMENT_CLAUSE, merge(parsed));
  }
}
