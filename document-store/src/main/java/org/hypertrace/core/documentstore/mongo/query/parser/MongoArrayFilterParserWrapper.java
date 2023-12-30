package org.hypertrace.core.documentstore.mongo.query.parser;

@FunctionalInterface
public interface MongoArrayFilterParserWrapper {
  MongoSelectTypeExpressionParser getParser(
      final MongoSelectTypeExpressionParser baseParser,
      final String arraySource,
      final String alias);
}
