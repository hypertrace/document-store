package org.hypertrace.core.documentstore.mongo;

import static java.util.function.Predicate.not;
import static org.hypertrace.core.documentstore.mongo.MongoPaginationHelper.applyPagination;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSortingExpressionParser.applySorting;

import com.mongodb.BasicDBObject;
import com.mongodb.Function;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.mongo.parser.MongoFilteringExpressionParser;
import org.hypertrace.core.documentstore.mongo.parser.MongoGroupingExpressionParser;
import org.hypertrace.core.documentstore.mongo.parser.MongoSelectingExpressionParser;
import org.hypertrace.core.documentstore.mongo.parser.MongoSortingExpressionParser;
import org.hypertrace.core.documentstore.query.Query;

public class MongoQueryExecutor {

  public static Iterator<Document> find(final Query query,
      final com.mongodb.client.MongoCollection<BasicDBObject> collection,
      final Function<BasicDBObject, Document> convertor) {

    BasicDBObject filterClause = MongoFilteringExpressionParser.getFilterClause(query.getFilter());
    Bson projection = MongoSelectingExpressionParser.getSelections(query.getSelections());

    FindIterable<BasicDBObject> iterable = collection.find(filterClause).projection(projection);
    applySorting(iterable, query.getSortingDefinitions());
    applyPagination(iterable, query.getPaginationDefinition());

    return getIteratorFromCursor(iterable.cursor(), convertor);
  }

  public static Iterator<Document> aggregate(final Query query,
      final com.mongodb.client.MongoCollection<BasicDBObject> collection,
      final Function<BasicDBObject, Document> convertor) {
    BasicDBObject filterClause = MongoFilteringExpressionParser.getFilterClause(query.getFilter());
    BasicDBObject groupFilterClause = MongoFilteringExpressionParser.getFilterClause(
        query.getAggregationFilter());
    BasicDBObject groupClause = MongoGroupingExpressionParser.getGroupClause(query.getSelections(),
        query.getAggregations());
    BasicDBObject sortClause = MongoSortingExpressionParser.getSortClause(
        query.getSortingDefinitions());

    List<BasicDBObject> pipeline = Stream
        .of(filterClause, groupClause, groupFilterClause, sortClause)
        .filter(not(BasicDBObject::isEmpty))
        .collect(Collectors.toList());

    AggregateIterable<BasicDBObject> iterable = collection.aggregate(pipeline);

    return getIteratorFromCursor(iterable.cursor(), convertor);
  }

  private static Iterator<Document> getIteratorFromCursor(final MongoCursor<BasicDBObject> cursor,
      final Function<BasicDBObject, Document> convertor) {
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
