package org.hypertrace.core.documentstore.mongo.query.parser;

public class MongoArrayRelationalFilterParserWrapper implements MongoArrayFilterParserGetter {

  @Override
  public MongoSelectTypeExpressionParser getParser(final String arraySource, final String alias) {
    // Substitute the array name in the LHS with the alias (because it could be encoded)
    // and then wrap with dollar ($) twice
    return new MongoIdentifierPrefixingParser(
        new MongoIdentifierPrefixingParser(
            new MongoIdentifierSubstitutingParser(
                new MongoIdentifierExpressionParser(), arraySource, alias)));
  }
}
