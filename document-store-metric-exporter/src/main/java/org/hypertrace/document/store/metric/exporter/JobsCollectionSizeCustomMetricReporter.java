package org.hypertrace.document.store.metric.exporter;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.document.store.metric.exporter.CommonMetricExporter.VALUE_KEY;

import java.time.Duration;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.query.Query;

public class JobsCollectionSizeCustomMetricReporter implements CommonCustomMetricQueryProvider {

  @Override
  public String collectionName() {
    return "jobs";
  }

  @Override
  public String metricName() {
    return "jobs.collection.size";
  }

  @Override
  public Query getQuery() {
    return Query.builder()
        .addSelection(IdentifierExpression.of("tenantId"), "tenant_id")
        .addSelection(IdentifierExpression.of("jobType"), "job_type")
        .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), VALUE_KEY)
        .addAggregation(IdentifierExpression.of("tenantId"))
        .addAggregation(IdentifierExpression.of("jobType"))
        .build();
  }

  @Override
  public Duration reportingInterval() {
    return Duration.ofHours(1);
  }
}
