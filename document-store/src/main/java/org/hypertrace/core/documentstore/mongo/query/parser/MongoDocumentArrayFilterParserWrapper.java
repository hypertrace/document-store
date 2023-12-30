package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.FIELD_SEPARATOR;

public class MongoDocumentArrayFilterParserWrapper implements MongoArrayFilterParserWrapper {

  @Override
  public MongoSelectTypeExpressionParser getParser(
      final MongoSelectTypeExpressionParser baseParser,
      final String arraySource,
      final String alias) {
    // Substitute the array name in the LHS with the alias (because it could be encoded)
    // and then wrap with dollar ($) twice. E.g.: 'name' --> '$$planets.name'
    return new MongoIdentifierPrefixingParser(
        new MongoIdentifierPrefixingParser(
            new MongoIdentifierPrefixingParser(baseParser, alias + FIELD_SEPARATOR)));
  }
}
