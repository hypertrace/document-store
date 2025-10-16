package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.hypertrace.core.documentstore.expression.type.GroupTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SortTypeExpression;
import org.hypertrace.core.documentstore.parser.GroupTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

/**
 * Expression representing either an identifier/column name
 *
 * <p>Example: IdentifierExpression.of("col1");
 *
 * <p><strong>Security Note:</strong> Field names are validated to prevent SQL injection attacks, as
 * they are not parameterized in PreparedStatements.
 */
@Value
@NonFinal
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class IdentifierExpression
    implements GroupTypeExpression, SelectTypeExpression, SortTypeExpression {

  String name;

  public static IdentifierExpression of(final String name) {
    Preconditions.checkArgument(name != null && !name.isBlank(), "name is null or blank");
    
    // Validate to prevent SQL injection - field names can contain dots for nested paths
    PostgresUtils.validateIdentifier(name, "Identifier name");
    
    return new IdentifierExpression(name);
  }

  @Override
  public <T> T accept(final GroupTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(final SelectTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(final SortTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "`" + name + "`";
  }
}
