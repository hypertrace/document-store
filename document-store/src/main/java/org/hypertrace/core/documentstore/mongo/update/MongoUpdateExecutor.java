package org.hypertrace.core.documentstore.mongo.update;

import static org.hypertrace.core.documentstore.model.options.DataFreshness.REALTIME_FRESHNESS;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.NONE;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getReturnDocument;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoFilterTypeExpressionParser.getFilter;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSelectTypeExpressionParser.getSelections;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSortTypeExpressionParser.getOrders;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.commons.CommonUpdateValidator;
import org.hypertrace.core.documentstore.commons.UpdateValidator;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
import org.hypertrace.core.documentstore.model.options.ReturnDocumentType;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.options.UpdateOptions.MissingDocumentStrategy;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.mongo.MongoUtils;
import org.hypertrace.core.documentstore.mongo.query.MongoQueryExecutor;
import org.hypertrace.core.documentstore.mongo.update.parser.MongoUpdateParser;
import org.hypertrace.core.documentstore.query.Query;

@Slf4j
public class MongoUpdateExecutor {
  private final com.mongodb.client.MongoCollection<BasicDBObject> collection;
  private final MongoUpdateParser updateParser;
  private final MongoQueryExecutor queryExecutor;
  private final UpdateValidator updateValidator;

  public MongoUpdateExecutor(
      final MongoCollection<BasicDBObject> collection, ConnectionConfig connectionConfig) {
    this.collection = collection;
    this.updateParser = new MongoUpdateParser(Clock.systemUTC());
    this.queryExecutor = new MongoQueryExecutor(collection, connectionConfig);
    this.updateValidator = new CommonUpdateValidator();
  }

  public Optional<Document> update(
      final Query query,
      final Collection<SubDocumentUpdate> updates,
      final UpdateOptions updateOptions)
      throws IOException {

    updateValidator.validate(updates);

    try {
      final BasicDBObject selections = getSelections(query);
      final BasicDBObject sorts = getOrders(query);
      final FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
      options.upsert(
          updateOptions
              .getMissingDocumentStrategy()
              .equals(MissingDocumentStrategy.CREATE_USING_UPDATES));
      final ReturnDocumentType returnDocumentType = updateOptions.getReturnDocumentType();

      options.returnDocument(getReturnDocument(returnDocumentType));

      if (!selections.isEmpty()) {
        options.projection(selections);
      }

      if (!sorts.isEmpty()) {
        options.sort(sorts);
      }

      final BasicDBObject filter = getFilter(query, Query::getFilter);
      final BasicDBObject updateObject = updateParser.buildUpdateClause(updates);

      if (returnDocumentType == NONE) {
        collection.findOneAndUpdate(filter, updateObject, options);
        return Optional.empty();
      }

      return Optional.ofNullable(collection.findOneAndUpdate(filter, updateObject, options))
          .map(MongoUtils::dbObjectToDocument);
    } catch (final Exception e) {
      throw new IOException(e);
    }
  }

  public Optional<MongoCursor<BasicDBObject>> bulkUpdate(
      final Query query,
      final Collection<SubDocumentUpdate> updates,
      final UpdateOptions updateOptions)
      throws IOException {
    updateValidator.validate(updates);

    final BasicDBObject filter = getFilter(query, Query::getFilter);
    final BasicDBObject updateObject = updateParser.buildUpdateClause(updates);
    final ReturnDocumentType returnDocumentType = updateOptions.getReturnDocumentType();
    final com.mongodb.client.model.UpdateOptions mongoUpdateOptions =
        new com.mongodb.client.model.UpdateOptions();
    mongoUpdateOptions.upsert(
        updateOptions
            .getMissingDocumentStrategy()
            .equals(MissingDocumentStrategy.CREATE_USING_UPDATES));
    final MongoCursor<BasicDBObject> cursor;

    switch (returnDocumentType) {
      case BEFORE_UPDATE:
        cursor =
            queryExecutor.aggregate(
                query, QueryOptions.builder().dataFreshness(REALTIME_FRESHNESS).build());
        logAndUpdate(filter, updateObject, mongoUpdateOptions);
        return Optional.of(cursor);

      case AFTER_UPDATE:
        logAndUpdate(filter, updateObject, mongoUpdateOptions);
        cursor =
            queryExecutor.aggregate(
                query, QueryOptions.builder().dataFreshness(REALTIME_FRESHNESS).build());
        return Optional.of(cursor);

      case NONE:
        logAndUpdate(filter, updateObject, mongoUpdateOptions);
        return Optional.empty();

      default:
        throw new IOException("Unrecognized return document type: " + returnDocumentType);
    }
  }

  private void logAndUpdate(
      final BasicDBObject filter,
      final BasicDBObject setObject,
      com.mongodb.client.model.UpdateOptions updateOptions)
      throws IOException {
    try {
      log.debug(
          "Updating {} using {} with filter {} and updateOptions: {}",
          collection.getNamespace(),
          setObject,
          filter,
          updateOptions);
      collection.updateMany(filter, setObject, updateOptions);
    } catch (Exception e) {
      throw new IOException("Error while updating", e);
    }
  }
}
