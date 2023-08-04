package org.hypertrace.document.store.metric.exporter;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.mongodb.client.MongoClient;
import com.typesafe.config.Config;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.document.store.metric.exporter.mongo.MongoConnectionCustomMetricQueryProvider;
import org.hypertrace.document.store.metric.exporter.mongo.MongoConnectionProvider;

@AllArgsConstructor
public class DocStoreMetricExporterModule extends AbstractModule {
  private final Config config;

  @Override
  protected void configure() {
    bind(Config.class).toInstance(config);
    bind(MongoClient.class).toProvider(MongoConnectionProvider.class).in(Singleton.class);

    final Multibinder<CommonCustomMetricQueryProvider> commonCustomMetricProviderBinder =
        Multibinder.newSetBinder(binder(), CommonCustomMetricQueryProvider.class);
    commonCustomMetricProviderBinder.addBinding().to(JobsCollectionSizeCustomMetricReporter.class);

    final Multibinder<DBCustomMetricValueProvider> dbCustomMetricProviderBinder =
        Multibinder.newSetBinder(binder(), DBCustomMetricValueProvider.class);
    if (config.hasPath("document.store.mongo")) {
      dbCustomMetricProviderBinder.addBinding().to(MongoConnectionCustomMetricQueryProvider.class);
    }

    final Multibinder<MetricExporter> metricExporterBinder =
        Multibinder.newSetBinder(binder(), MetricExporter.class);
    metricExporterBinder.addBinding().to(CommonMetricExporter.class);
    metricExporterBinder.addBinding().to(DBMetricExporter.class);
  }

  @Provides
  @Singleton
  public Datastore provideDatastore() {
    final Config docStoreConfig = config.getConfig("document.store");
    final String dataStoreType = docStoreConfig.getString("dataStoreType");
    return DatastoreProvider.getDatastore(dataStoreType, docStoreConfig.getConfig(dataStoreType));
  }
}
