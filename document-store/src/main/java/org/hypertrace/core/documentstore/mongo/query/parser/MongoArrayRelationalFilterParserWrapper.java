package org.hypertrace.core.documentstore.mongo.query.parser;

public class MongoArrayRelationalFilterParserWrapper implements MongoArrayFilterParserWrapper {

  @Override
  public MongoSelectTypeExpressionParser getParser(
      final MongoSelectTypeExpressionParser baseParser,
      final String arraySource,
      final String alias) {
    // Substitute the array name in the LHS with the alias (because it could be encoded)
    // and then wrap with dollar ($) twice
    return new MongoIdentifierPrefixingParser(
        new MongoIdentifierPrefixingParser(
            new MongoIdentifierSubstitutingParser(baseParser, arraySource, alias)));
  }
}
