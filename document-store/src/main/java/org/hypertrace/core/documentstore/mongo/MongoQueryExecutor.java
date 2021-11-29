package org.hypertrace.core.documentstore.mongo;

import static java.util.function.Predicate.not;
import static org.hypertrace.core.documentstore.mongo.MongoPaginationHelper.applyPagination;
import static org.hypertrace.core.documentstore.mongo.MongoPaginationHelper.getLimitClause;
import static org.hypertrace.core.documentstore.mongo.MongoPaginationHelper.getSkipClause;
import static org.hypertrace.core.documentstore.mongo.parser.MongoFilteringExpressionParser.getFilter;
import static org.hypertrace.core.documentstore.mongo.parser.MongoFilteringExpressionParser.getFilterClause;
import static org.hypertrace.core.documentstore.mongo.parser.MongoGroupingExpressionParser.getGroupClause;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSelectingExpressionParser.getProjectClause;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSelectingExpressionParser.getSelections;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSortingExpressionParser.getOrders;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSortingExpressionParser.getSortClause;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.mongo.query.transformer.MongoQueryTransformer;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.QueryInternal;

@Slf4j
@AllArgsConstructor
public class MongoQueryExecutor {
  final com.mongodb.client.MongoCollection<BasicDBObject> collection;

  public MongoCursor<BasicDBObject> find(final Query originalQuery) {
    QueryInternal query = (QueryInternal) originalQuery;

    BasicDBObject filterClause = getFilter(query, QueryInternal::getFilter);
    BasicDBObject projection = getSelections(query);

    FindIterable<BasicDBObject> iterable = collection.find(filterClause).projection(projection);

    BasicDBObject sortOrders = getOrders(query);
    if (!sortOrders.isEmpty()) {
      iterable.sort(sortOrders);
    }

    applyPagination(iterable, query);

    logClauses(query, projection, filterClause, sortOrders, query.getPagination().orElse(null));

    return iterable.cursor();
  }

  public MongoCursor<BasicDBObject> aggregate(final Query originalQuery) {
    QueryInternal query = transformAndLog(originalQuery);

    BasicDBObject filterClause = getFilterClause(query, QueryInternal::getFilter);
    BasicDBObject groupFilterClause = getFilterClause(query, QueryInternal::getAggregationFilter);

    BasicDBObject groupClause = getGroupClause(query);
    BasicDBObject sortClause = getSortClause(query);

    BasicDBObject skipClause = getSkipClause(query);
    BasicDBObject limitClause = getLimitClause(query);

    BasicDBObject projectClause = getProjectClause(query);

    List<BasicDBObject> pipeline =
        Stream.of(
                filterClause,
                groupClause,
                projectClause,
                groupFilterClause,
                sortClause,
                skipClause,
                limitClause)
            .filter(not(BasicDBObject::isEmpty))
            .collect(Collectors.toList());

    logPipeline(pipeline);
    AggregateIterable<BasicDBObject> iterable = collection.aggregate(pipeline);

    return iterable.cursor();
  }

  private void logClauses(
      final QueryInternal query,
      final Bson projection,
      final Bson filterClause,
      final Bson sortOrders,
      final Pagination pagination) {
    log.debug(
        "MongoDB find():\nQuery: {}\nCollection: {}\n Projections: {}\n Filter: {}\n Sorting: {}\n Pagination: {}",
        query,
        collection.getNamespace(),
        projection,
        filterClause,
        sortOrders,
        pagination);
  }

  private void logPipeline(final List<BasicDBObject> pipeline) {
    log.debug(
        "MongoDB aggregation():\n Collection: {}\n Pipeline: {}",
        collection.getNamespace(),
        pipeline);
  }

  private QueryInternal transformAndLog(final Query originalQuery) {
    QueryInternal query = (QueryInternal) originalQuery;

    log.debug("MongoDB query before transformation: {}", query);
    query = MongoQueryTransformer.transform(query);
    log.debug("MongoDB query after transformation: {}", query);
    return query;
  }
}
