package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toUnmodifiableMap;

import com.mongodb.BasicDBObject;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import org.hypertrace.core.documentstore.query.Query;

public class MongoAddFieldsParser {
  private static final String ADD_FIELDS_CLAUSE = "$addFields";
  private static final List<Function<Query, Map<String, Object>>> newFieldsAdders =
      List.of(MongoAddFieldsParser::getNewFieldsForFiltering);

  public static BasicDBObject getAddFieldsClause(final Query query) {
    final BasicDBObject object = getAddFields(query);
    return object.isEmpty() ? new BasicDBObject() : new BasicDBObject(ADD_FIELDS_CLAUSE, object);
  }

  public static BasicDBObject getAddFields(final Query query) {
    return newFieldsAdders.stream()
        .map(fieldsAdder -> fieldsAdder.apply(query))
        .map(Map::entrySet)
        .flatMap(Set::stream)
        .collect(
            collectingAndThen(
                toUnmodifiableMap(Entry::getKey, Entry::getValue, (oldVal, newVal) -> oldVal),
                BasicDBObject::new));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> getNewFieldsForFiltering(final Query query) {
    final MongoAddFieldsFilterParser mongoAddFieldsFilterParser = new MongoAddFieldsFilterParser();
    return (Map<String, Object>)
        query
            .getFilter()
            .map(filter -> filter.accept(mongoAddFieldsFilterParser))
            .orElse(emptyMap());
  }
}
