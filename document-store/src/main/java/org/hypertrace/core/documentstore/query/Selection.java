package org.hypertrace.core.documentstore.query;

public interface Selection {

  boolean isAggregation();

  boolean allColumnsSelected();
}
