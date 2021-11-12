package org.hypertrace.core.documentstore.query;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AllSelection implements Selection {

  public static final AllSelection ALL = new AllSelection();

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public boolean allColumnsSelected() {
    return true;
  }
}
