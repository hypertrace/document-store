package org.hypertrace.core.documentstore.mongo.update;

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
import org.hypertrace.core.documentstore.model.options.ReturnDocumentType;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
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

  public MongoUpdateExecutor(final MongoCollection<BasicDBObject> collection) {
    this.collection = collection;
    this.updateParser = new MongoUpdateParser(Clock.systemUTC());
    this.queryExecutor = new MongoQueryExecutor(collection);
  }

  public Optional<Document> update(
      final Query query,
      final Collection<SubDocumentUpdate> updates,
      final UpdateOptions updateOptions)
      throws IOException {

    ensureAtLeastOneUpdateIsPresent(updates);

    try {
      final BasicDBObject selections = getSelections(query);
      final BasicDBObject sorts = getOrders(query);
      final FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
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
    ensureAtLeastOneUpdateIsPresent(updates);

    final BasicDBObject filter = getFilter(query, Query::getFilter);
    final BasicDBObject setObject = updateParser.buildUpdateClause(updates);
    final ReturnDocumentType returnDocumentType = updateOptions.getReturnDocumentType();
    final MongoCursor<BasicDBObject> cursor;

    switch (returnDocumentType) {
      case BEFORE_UPDATE:
        cursor = queryExecutor.aggregate(query);
        logAndUpdate(filter, setObject);
        return Optional.of(cursor);

      case AFTER_UPDATE:
        logAndUpdate(filter, setObject);
        cursor = queryExecutor.aggregate(query);
        return Optional.of(cursor);

      case NONE:
        logAndUpdate(filter, setObject);
        return Optional.empty();

      default:
        throw new IOException("Unrecognized return document type: " + returnDocumentType);
    }
  }

  private void logAndUpdate(final BasicDBObject filter, final BasicDBObject setObject) {
    log.debug("Updating {} using {} with filter {}", collection.getNamespace(), setObject, filter);
    collection.updateMany(filter, setObject);
  }

  private void ensureAtLeastOneUpdateIsPresent(Collection<SubDocumentUpdate> updates)
      throws IOException {
    if (updates.isEmpty()) {
      throw new IOException("At least one update is required");
    }
  }
}
