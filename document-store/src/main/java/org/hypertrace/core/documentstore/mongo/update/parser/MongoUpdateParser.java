package org.hypertrace.core.documentstore.mongo.update.parser;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toUnmodifiableList;

import com.mongodb.BasicDBObject;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.hypertrace.core.documentstore.model.subdoc.SubDocument;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.mongo.MongoUtils;

public class MongoUpdateParser {
  private static final Set<MongoOperationParser> OPERATOR_PARSERS =
      Set.of(
          new MongoAddToListIfAbsentOperationParser(),
          new MongoAppendToListOperationParser(),
          new MongoRemoveAllFromListOperationParser(),
          new MongoSetOperationParser(),
          new MongoAddOperationParser(),
          new MongoUnsetOperationParser());

  private final Clock clock;

  public MongoUpdateParser(final Clock clock) {
    this.clock = clock;
  }

  public BasicDBObject buildUpdateClause(final Collection<SubDocumentUpdate> updates) {
    final List<SubDocumentUpdate> allUpdates =
        Stream.concat(updates.stream(), Stream.of(getLastUpdatedTimeUpdate()))
            .collect(toUnmodifiableList());
    return OPERATOR_PARSERS.stream()
        .map(parser -> parser.parse(allUpdates))
        .filter(not(BasicDBObject::isEmpty))
        .collect(collectingAndThen(toUnmodifiableList(), MongoUtils::merge));
  }

  private SubDocumentUpdate getLastUpdatedTimeUpdate() {
    return SubDocumentUpdate.of(SubDocument.implicitUpdatedTime(), clock.millis());
  }
}
