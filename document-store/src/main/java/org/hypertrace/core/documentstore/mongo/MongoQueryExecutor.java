package org.hypertrace.core.documentstore.mongo;

import static java.util.function.Predicate.not;
import static org.hypertrace.core.documentstore.mongo.MongoPaginationHelper.applyPagination;
import static org.hypertrace.core.documentstore.mongo.MongoPaginationHelper.getLimitClause;
import static org.hypertrace.core.documentstore.mongo.MongoPaginationHelper.getSkipClause;
import static org.hypertrace.core.documentstore.mongo.parser.MongoFilteringExpressionParser.getFilterClause;
import static org.hypertrace.core.documentstore.mongo.parser.MongoGroupingExpressionParser.getGroupClause;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSelectingExpressionParser.getProjectClause;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSelectingExpressionParser.getSelections;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSortingExpressionParser.getOrders;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSortingExpressionParser.getSortClause;

import com.mongodb.BasicDBObject;
import com.mongodb.Function;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.query.PaginationDefinition;
import org.hypertrace.core.documentstore.query.Query;

@Slf4j
public class MongoQueryExecutor {

  public static Iterator<Document> find(
      final Query query,
      final com.mongodb.client.MongoCollection<BasicDBObject> collection,
      final Function<BasicDBObject, Document> convertor) {

    BasicDBObject filterClause = getFilterClause(query.getFilter());
    Bson projection = getSelections(query.getSelections());

    FindIterable<BasicDBObject> iterable = collection.find(filterClause).projection(projection);

    BasicDBObject sortOrders = getOrders(query.getSortingDefinitions());
    iterable.sort(sortOrders);

    applyPagination(iterable, query.getPaginationDefinition());

    logClauses(projection, filterClause, sortOrders, query.getPaginationDefinition());

    return getIteratorFromCursor(iterable.cursor(), convertor);
  }

  public static Iterator<Document> aggregate(
      final Query query,
      final com.mongodb.client.MongoCollection<BasicDBObject> collection,
      final Function<BasicDBObject, Document> convertor) {

    BasicDBObject filterClause = getFilterClause(query.getFilter());
    BasicDBObject groupFilterClause = getFilterClause(query.getAggregationFilter());

    BasicDBObject groupClause = getGroupClause(query.getSelections(), query.getAggregations());
    BasicDBObject sortClause = getSortClause(query.getSortingDefinitions());

    BasicDBObject skipClause = getSkipClause(query.getPaginationDefinition());
    BasicDBObject limitClause = getLimitClause(query.getPaginationDefinition());

    BasicDBObject projectClause = getProjectClause(query.getSelections());

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

    return getIteratorFromCursor(iterable.cursor(), convertor);
  }

  private static void logClauses(
      Bson projection,
      Bson filterClause,
      Bson sortOrders,
      PaginationDefinition paginationDefinition) {
    log.debug(
        "Projections: {} \n" + "Filter: {} \n" + "Sorting: {} \n" + "Pagination: {}",
        projection,
        filterClause,
        sortOrders,
        paginationDefinition);
  }

  private static void logPipeline(List<BasicDBObject> pipeline) {
    log.debug("Aggregation pipeline: {}", pipeline);
  }

  private static Iterator<Document> getIteratorFromCursor(
      final MongoCursor<BasicDBObject> cursor, final Function<BasicDBObject, Document> convertor) {
    return new Iterator<>() {

      @Override
      public boolean hasNext() {
        return cursor.hasNext();
      }

      @Override
      public Document next() {
        return convertor.apply(cursor.next());
      }
    };
  }
}
