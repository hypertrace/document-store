package org.hypertrace.core.documentstore.mongo.update.parser;

import static org.hypertrace.core.documentstore.commons.DocStoreConstants.LAST_UPDATED_TIME;

import com.mongodb.BasicDBObject;
import java.time.Clock;
import java.util.Collection;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.mongo.subdoc.MongoSubDocumentValueSanitizer;

public class MongoUpdateParser {
  private static final String SET_CLAUSE = "$set";

  private final Clock clock;

  public MongoUpdateParser(final Clock clock) {
    this.clock = clock;
  }

  public BasicDBObject buildSetClause(final Collection<SubDocumentUpdate> updates) {
    final BasicDBObject updateObject = parseUpdates(updates);
    addLastUpdatedTimeUpdate(updateObject);
    return getSetClause(updateObject);
  }

  private BasicDBObject parseUpdates(final Collection<SubDocumentUpdate> updates) {
    final BasicDBObject updateObject = new BasicDBObject();
    final MongoSubDocumentValueSanitizer sanitizer = new MongoSubDocumentValueSanitizer();

    for (final SubDocumentUpdate update : updates) {
      final String path = update.getSubDocument().getPath();
      final Object value = update.getSubDocumentValue().accept(sanitizer);
      updateObject.put(path, value);
    }

    return updateObject;
  }

  private void addLastUpdatedTimeUpdate(final BasicDBObject updates) {
    updates.append(LAST_UPDATED_TIME, clock.millis());
  }

  private BasicDBObject getSetClause(final BasicDBObject updates) {
    return new BasicDBObject(SET_CLAUSE, updates);
  }
}
