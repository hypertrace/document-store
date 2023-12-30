package org.hypertrace.core.documentstore.mongo.query;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import java.util.Optional;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;

public class MongoPaginationHelper {

  private static final String SKIP_CLAUSE = "$skip";
  private static final String LIMIT_CLAUSE = "$limit";

  static BasicDBObject getSkipClause(final Query query) {
    Optional<Pagination> paginationOptional =
        query.getPagination().filter(pagination -> pagination.getOffset() > 0);
    return paginationOptional
        .map(pagination -> new BasicDBObject(SKIP_CLAUSE, pagination.getOffset()))
        .orElse(new BasicDBObject());
  }

  static BasicDBObject getLimitClause(final Query query) {
    Optional<Pagination> paginationOptional =
        query.getPagination().filter(pagination -> pagination.getLimit() > 0);
    return paginationOptional
        .map(pagination -> new BasicDBObject(LIMIT_CLAUSE, pagination.getLimit()))
        .orElse(new BasicDBObject());
  }

  static void applyPagination(final FindIterable<BasicDBObject> iterable, final Query query) {
    Optional<Pagination> paginationOptional = query.getPagination();
    paginationOptional.ifPresent(
        pagination -> iterable.skip(pagination.getOffset()).limit(pagination.getLimit()));
  }
}
