package org.hypertrace.core.documentstore.mongo.query.parser;

@FunctionalInterface
public interface MongoArrayFilterParserGetter {
  MongoSelectTypeExpressionParser getParser(final String arraySource, final String alias);
}
