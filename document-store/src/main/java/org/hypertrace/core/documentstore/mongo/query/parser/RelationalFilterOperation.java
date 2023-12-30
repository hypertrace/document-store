package org.hypertrace.core.documentstore.mongo.query.parser;

import java.util.Map;
import java.util.function.BiFunction;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

@FunctionalInterface
interface RelationalFilterOperation
    extends BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>> {}
