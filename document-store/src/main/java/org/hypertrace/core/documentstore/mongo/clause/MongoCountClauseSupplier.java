package org.hypertrace.core.documentstore.mongo.clause;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;

import com.mongodb.BasicDBObject;

public class MongoCountClauseSupplier {
  public static final String COUNT_ALIAS = "count";
  private static final String COUNT_CLAUSE = PREFIX + "count";

  public static BasicDBObject getCountClause() {
    return new BasicDBObject(COUNT_CLAUSE, COUNT_ALIAS);
  }
}
