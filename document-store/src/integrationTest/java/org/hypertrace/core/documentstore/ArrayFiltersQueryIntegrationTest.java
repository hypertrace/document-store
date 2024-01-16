package org.hypertrace.core.documentstore;

import static org.hypertrace.core.documentstore.expression.impl.LogicalExpression.and;
import static org.hypertrace.core.documentstore.expression.operators.ArrayOperator.ANY;
import static org.hypertrace.core.documentstore.model.config.DatabaseType.MONGO;
import static org.hypertrace.core.documentstore.model.config.DatabaseType.POSTGRES;
import static org.hypertrace.core.documentstore.utils.Utils.MONGO_STORE;
import static org.hypertrace.core.documentstore.utils.Utils.POSTGRES_STORE;

import com.google.common.io.Resources;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.ArrayRelationalFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.DocumentArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials;
import org.hypertrace.core.documentstore.model.config.DatastoreConfig;
import org.hypertrace.core.documentstore.model.config.Endpoint;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.utils.Utils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.com.google.common.collect.Maps;
import org.testcontainers.utility.DockerImageName;

class ArrayFiltersQueryIntegrationTest {
  private static final String COLLECTION_NAME = "galaxy";

  private static Map<String, Datastore> datastoreMap;

  private static GenericContainer<?> mongo;
  private static GenericContainer<?> postgres;

  @BeforeAll
  public static void init() throws IOException {
    datastoreMap = Maps.newHashMap();
    initializeAndConnectToMongo();
    initializeAndConnectToPostgres();

    createCollectionData();
  }

  private static void initializeAndConnectToPostgres() {
    postgres =
        new GenericContainer<>(DockerImageName.parse("postgres:13.1"))
            .withEnv("POSTGRES_PASSWORD", "postgres")
            .withEnv("POSTGRES_USER", "postgres")
            .withExposedPorts(5432)
            .waitingFor(Wait.forListeningPort());
    postgres.start();

    Datastore postgresDatastore =
        DatastoreProvider.getDatastore(
            DatastoreConfig.builder()
                .type(POSTGRES)
                .connectionConfig(
                    ConnectionConfig.builder()
                        .type(POSTGRES)
                        .addEndpoint(
                            Endpoint.builder()
                                .host("localhost")
                                .port(postgres.getMappedPort(5432))
                                .build())
                        .credentials(
                            ConnectionCredentials.builder()
                                .username("postgres")
                                .password("postgres")
                                .build())
                        .build())
                .build());

    datastoreMap.put(POSTGRES_STORE, postgresDatastore);
  }

  private static void initializeAndConnectToMongo() {
    mongo =
        new GenericContainer<>(DockerImageName.parse("mongo:4.4.0"))
            .withExposedPorts(27017)
            .waitingFor(Wait.forListeningPort());
    mongo.start();

    Datastore mongoDatastore =
        DatastoreProvider.getDatastore(
            DatastoreConfig.builder()
                .type(MONGO)
                .connectionConfig(
                    ConnectionConfig.builder()
                        .type(MONGO)
                        .addEndpoint(
                            Endpoint.builder()
                                .host("localhost")
                                .port(mongo.getMappedPort(27017))
                                .build())
                        .build())
                .build());
    datastoreMap.put(MONGO_STORE, mongoDatastore);
  }

  private static void createCollectionData() throws IOException {
    final Map<Key, Document> documents =
        Utils.buildDocumentsFromResource("query/array_operators/galaxy.json");
    datastoreMap.forEach(
        (k, v) -> {
          v.deleteCollection(ArrayFiltersQueryIntegrationTest.COLLECTION_NAME);
          v.createCollection(ArrayFiltersQueryIntegrationTest.COLLECTION_NAME, null);
          Collection collection = v.getCollection(ArrayFiltersQueryIntegrationTest.COLLECTION_NAME);
          collection.bulkUpsert(documents);
        });
  }

  @AfterAll
  public static void shutdown() {
    mongo.stop();
    postgres.stop();
  }

  private static class AllProvider implements ArgumentsProvider {
    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(Arguments.of(MONGO_STORE), Arguments.of(POSTGRES_STORE));
    }
  }

  @SuppressWarnings("unused")
  private static class MongoProvider implements ArgumentsProvider {
    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(Arguments.of(MONGO_STORE));
    }
  }

  @SuppressWarnings("unused")
  private static class PostgresProvider implements ArgumentsProvider {
    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(Arguments.of(POSTGRES_STORE));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  void getAllSolarSystemsWithAtLeastOnePlanetHavingBothWaterAndOxygen(final String dataStoreName)
      throws JSONException {
    final Collection collection = getCollection(dataStoreName);

    final Query query =
        Query.builder()
            .setFilter(
                DocumentArrayFilterExpression.builder()
                    .operator(ANY)
                    .arraySource(IdentifierExpression.of("additional_info.planets"))
                    .filter(
                        and(
                            ArrayRelationalFilterExpression.builder()
                                .operator(ANY)
                                .filter(
                                    RelationalExpression.of(
                                        IdentifierExpression.of("elements"),
                                        RelationalOperator.EQ,
                                        ConstantExpression.of("Oxygen")))
                                .build(),
                            ArrayRelationalFilterExpression.builder()
                                .operator(ANY)
                                .filter(
                                    RelationalExpression.of(
                                        IdentifierExpression.of("elements"),
                                        RelationalOperator.EQ,
                                        ConstantExpression.of("Water")))
                                .build(),
                            RelationalExpression.of(
                                IdentifierExpression.of("meta.num_moons"),
                                RelationalOperator.GT,
                                ConstantExpression.of(0))))
                    .build())
            .build();

    final Iterator<Document> documents = collection.aggregate(query);
    final String expected = readResource("at_least_one_planet_having_both_oxygen_and_water.json");
    final String actual = iteratorToJson(documents);

    JSONAssert.assertEquals(expected, actual, JSONCompareMode.LENIENT);
  }

  private String readResource(final String fileName) {
    try {
      return new String(
          Resources.getResource("query/array_operators/" + fileName).openStream().readAllBytes());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Collection getCollection(final String dataStoreName) {
    final Datastore datastore = datastoreMap.get(dataStoreName);
    return datastore.getCollection(ArrayFiltersQueryIntegrationTest.COLLECTION_NAME);
  }

  private String iteratorToJson(final Iterator<Document> iterator) {
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
        .map(Document::toJson)
        .map(
            json -> {
              try {
                return new JSONObject(json);
              } catch (JSONException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(
            Collector.of(
                JSONArray::new, JSONArray::put, (array1, array2) -> array1, JSONArray::toString));
  }
}
