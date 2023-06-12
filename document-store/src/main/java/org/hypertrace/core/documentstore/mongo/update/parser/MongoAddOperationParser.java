package org.hypertrace.core.documentstore.mongo.update.parser;

import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.ADD;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.merge;

import com.mongodb.BasicDBObject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;

@AllArgsConstructor
public class MongoAddOperationParser extends MongoOperationParser {
  private static final String INCREMENT_CLAUSE = "$inc";

  @Override
  UpdateOperator operator() {
    return ADD;
  }

  @Override
  BasicDBObject wrapWithOperator(List<BasicDBObject> parsed) {
    return new BasicDBObject(INCREMENT_CLAUSE, merge(parsed));
  }
}
