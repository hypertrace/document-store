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
import com.mongodb.client.model.Projections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;

@Slf4j
@AllArgsConstructor
public class MongoQueryExecutor {
  final com.mongodb.client.MongoCollection<BasicDBObject> collection;

  public MongoCursor<BasicDBObject> find(final Query query) {

    BasicDBObject filterClause = getFilter(query, Query::getFilter);
    Bson projection = Projections.include(getSelections(query));

    FindIterable<BasicDBObject> iterable = collection.find(filterClause).projection(projection);

    BasicDBObject sortOrders = getOrders(query);
    if (!sortOrders.isEmpty()) {
      iterable.sort(sortOrders);
    }

    applyPagination(iterable, query);

    logClauses(projection, filterClause, sortOrders, query.getPagination().orElse(null));

    return iterable.cursor();
  }

  public MongoCursor<BasicDBObject> aggregate(final Query query) {

    BasicDBObject filterClause = getFilterClause(query, Query::getFilter);
    BasicDBObject groupFilterClause = getFilterClause(query, Query::getAggregationFilter);

    BasicDBObject groupClause = getGroupClause(query);
    BasicDBObject sortClause = getSortClause(query);

    BasicDBObject skipClause = getSkipClause(query);
    BasicDBObject limitClause = getLimitClause(query);

    BasicDBObject projectClause = getProjectClause(query);

    List<BasicDBObject> pipeline =
        Stream.of(
                filterClause,
                groupClause,
                groupFilterClause,
                sortClause,
                skipClause,
                limitClause,
                projectClause)
            .filter(not(BasicDBObject::isEmpty))
            .collect(Collectors.toList());

    logPipeline(pipeline);
    AggregateIterable<BasicDBObject> iterable = collection.aggregate(pipeline);

    return iterable.cursor();
  }

  private void logClauses(
      Bson projection, Bson filterClause, Bson sortOrders, Pagination pagination) {
    log.debug(
        "MongoDB find():\nCollection: {}\n Projections: {}\n Filter: {}\n Sorting: {}\n Pagination: {}",
        collection.getNamespace(),
        projection,
        filterClause,
        sortOrders,
        pagination);
  }

  private void logPipeline(List<BasicDBObject> pipeline) {
    log.debug(
        "MongoDB aggregation():\n Collection: {}\n Pipeline: {}",
        collection.getNamespace(),
        pipeline);
  }
}
