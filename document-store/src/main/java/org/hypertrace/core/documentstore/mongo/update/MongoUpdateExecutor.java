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
import org.hypertrace.core.documentstore.commons.CommonUpdateValidator;
import org.hypertrace.core.documentstore.commons.UpdateValidator;
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
  private final UpdateValidator updateValidator;

  public MongoUpdateExecutor(final MongoCollection<BasicDBObject> collection) {
    this.collection = collection;
    this.updateParser = new MongoUpdateParser(Clock.systemUTC());
    this.queryExecutor = new MongoQueryExecutor(collection);
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
    final MongoCursor<BasicDBObject> cursor;

    switch (returnDocumentType) {
      case BEFORE_UPDATE:
        cursor = queryExecutor.aggregate(query);
        logAndUpdate(filter, updateObject);
        return Optional.of(cursor);

      case AFTER_UPDATE:
        logAndUpdate(filter, updateObject);
        cursor = queryExecutor.aggregate(query);
        return Optional.of(cursor);

      case NONE:
        logAndUpdate(filter, updateObject);
        return Optional.empty();

      default:
        throw new IOException("Unrecognized return document type: " + returnDocumentType);
    }
  }

  private void logAndUpdate(final BasicDBObject filter, final BasicDBObject setObject)
      throws IOException {
    try {
      log.debug(
          "Updating {} using {} with filter {}", collection.getNamespace(), setObject, filter);
      collection.updateMany(filter, setObject);
    } catch (Exception e) {
      throw new IOException("Error while updating", e);
    }
  }
}
