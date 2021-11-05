package org.hypertrace.core.documentstore;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.Expression;

@Data
@AllArgsConstructor
public class GroupBy {
  // TODO: Implement a builder for easily constructing the "group by" clause
  private Expression key; // The key field whose distinct values would be returned
  private List<GroupingSpec> groupingSpecs; // The list of aggregations required

  public enum Accumulator {
    FIRST,
    LAST,
    MIN,
    MAX,
    COUNT,
    SUM,
    AVERAGE,
  }

  @Override
  public String toString() {
    return "GroupBy{"
        + "key="
        + key
        + ", groupingSpecs="
        + StringUtils.join(groupingSpecs, ",")
        + '}';
  }
}
