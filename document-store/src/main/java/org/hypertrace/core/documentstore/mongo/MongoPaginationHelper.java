package org.hypertrace.core.documentstore.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import org.hypertrace.core.documentstore.query.PaginationDefinition;

public class MongoPaginationHelper {

  static void applyPagination(
      FindIterable<BasicDBObject> iterable, PaginationDefinition paginationDefinition) {
    if (paginationDefinition != null) {
      iterable.skip(paginationDefinition.getOffset()).limit(paginationDefinition.getLimit());
    }
  }
}
