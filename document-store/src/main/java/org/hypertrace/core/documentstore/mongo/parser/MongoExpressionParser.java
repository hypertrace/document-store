package org.hypertrace.core.documentstore.mongo.parser;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.query.Query;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class MongoExpressionParser {
  protected final Query query;
}
