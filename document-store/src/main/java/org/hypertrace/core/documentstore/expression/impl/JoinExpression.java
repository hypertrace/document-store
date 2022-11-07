package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

/**
 * This expression allows joining two collections
 *
 * <p><code>
 *  JoinExpression.builder()
 *    .joiningCollectionName('sales_info')
 *    .joinCondition(
 *      and(
 *        RelationalExpression.of(
 *          SubQueryIdentifierExpression.of("order_id"),
 *          EQ,
 *          IdentifierExpression.of("order.id"))),
 *        RelationalExpression.of(
 *          SubQueryIdentifierExpression.of("order_type"),
 *          EQ,
 *          ConstantExpression.of("ONLINE")))))
 *    .collectionAlias('salesInfo')
 *    .build();
 *  </code>
 */
@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JoinExpression implements FromTypeExpression {

  String joiningCollectionName;
  @Nullable Query subQuery;
  @Nullable FilterTypeExpression joinCondition;
  String collectionAlias;

  @SuppressWarnings("unused")
  public static class JoinExpressionBuilder {
    public JoinExpression build() {
      Preconditions.checkArgument(joiningCollectionName != null, "Joining collection name is null");
      Preconditions.checkArgument(
          subQuery != null || joinCondition != null, "Sub-query and join condition both are null");
      Preconditions.checkArgument(collectionAlias != null, "Collection alias is null");
      return new JoinExpression(joiningCollectionName, subQuery, joinCondition, collectionAlias);
    }
  }

  @Override
  public <T> T accept(final FromTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return String.format(
        "LEFT OUTER JOIN %s AS %s ON (%s) USING %s",
        joiningCollectionName, collectionAlias, joinCondition, subQuery);
  }
}
