package org.hypertrace.core.documentstore.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import org.hypertrace.core.documentstore.query.PaginationDefinition;

public class MongoPaginationHelper {

  private static final String SKIP_CLAUSE = "$skip";
  private static final String LIMIT_CLAUSE = "$limit";

  static BasicDBObject getSkipClause(PaginationDefinition paginationDefinition) {
    if (paginationDefinition == null) {
      return new BasicDBObject();
    }

    return new BasicDBObject(SKIP_CLAUSE, paginationDefinition.getOffset());
  }

  static BasicDBObject getLimitClause(PaginationDefinition paginationDefinition) {
    if (paginationDefinition == null) {
      return new BasicDBObject();
    }

    return new BasicDBObject(LIMIT_CLAUSE, paginationDefinition.getLimit());
  }

  static void applyPagination(
      FindIterable<BasicDBObject> iterable, PaginationDefinition paginationDefinition) {
    if (paginationDefinition != null) {
      iterable.skip(paginationDefinition.getOffset()).limit(paginationDefinition.getLimit());
    }
  }
}
