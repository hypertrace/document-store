package org.hypertrace.core.documentstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.ConfigFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.model.options.MissingColumnStrategy;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.hypertrace.core.documentstore.query.Query;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Base class for write tests */
public abstract class BaseWriteTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseWriteTest.class);
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  protected static final String DEFAULT_TENANT = "default";

  // MongoDB container and datastore - shared by all subclasses
  protected static GenericContainer<?> mongoContainer;
  protected static Datastore mongoDatastore;

  // PostgreSQL container and datastore - shared by all subclasses
  protected static GenericContainer<?> postgresContainer;
  protected static Datastore postgresDatastore;

  // Maps for multi-store tests
  protected static Map<String, Datastore> datastoreMap = new HashMap<>();
  protected static Map<String, Collection> collectionMap = new HashMap<>();

  protected Collection getCollection(String storeName) {
    return collectionMap.get(storeName);
  }

  private static final String FLAT_COLLECTION_SCHEMA_PATH =
      "schema/flat_collection_test_schema.sql";

  protected static String loadFlatCollectionSchema() {
    try (InputStream is =
            BaseWriteTest.class.getClassLoader().getResourceAsStream(FLAT_COLLECTION_SCHEMA_PATH);
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append(" ");
      }
      return sb.toString().trim();
    } catch (Exception e) {
      throw new RuntimeException("Failed to load schema from " + FLAT_COLLECTION_SCHEMA_PATH, e);
    }
  }

  protected static void initMongo() {
    mongoContainer =
        new GenericContainer<>(DockerImageName.parse("mongo:8.0.1"))
            .withExposedPorts(27017)
            .waitingFor(Wait.forListeningPort());
    mongoContainer.start();

    Map<String, String> mongoConfig = new HashMap<>();
    mongoConfig.put("host", "localhost");
    mongoConfig.put("port", mongoContainer.getMappedPort(27017).toString());

    mongoDatastore = DatastoreProvider.getDatastore("Mongo", ConfigFactory.parseMap(mongoConfig));
    LOGGER.info("Mongo datastore initialized");
  }

  protected static void shutdownMongo() {
    if (mongoContainer != null) {
      mongoContainer.stop();
    }
  }

  protected static void initPostgres() {
    postgresContainer =
        new GenericContainer<>(DockerImageName.parse("postgres:13.1"))
            .withEnv("POSTGRES_PASSWORD", "postgres")
            .withEnv("POSTGRES_USER", "postgres")
            .withExposedPorts(5432)
            .waitingFor(Wait.forListeningPort());
    postgresContainer.start();

    String postgresConnectionUrl =
        String.format("jdbc:postgresql://localhost:%s/", postgresContainer.getMappedPort(5432));

    Map<String, String> postgresConfig = new HashMap<>();
    postgresConfig.put("url", postgresConnectionUrl);
    postgresConfig.put("user", "postgres");
    postgresConfig.put("password", "postgres");

    postgresDatastore =
        DatastoreProvider.getDatastore("Postgres", ConfigFactory.parseMap(postgresConfig));
    LOGGER.info("Postgres datastore initialized");
  }

  protected static void shutdownPostgres() {
    if (postgresContainer != null) {
      postgresContainer.stop();
    }
  }

  protected static void createFlatCollectionSchema(
      PostgresDatastore pgDatastore, String tableName) {
    String createTableSQL = String.format(loadFlatCollectionSchema(), tableName);

    try (Connection connection = pgDatastore.getPostgresClient();
        PreparedStatement statement = connection.prepareStatement(createTableSQL)) {
      statement.execute();
      LOGGER.info("Created flat collection table: {}", tableName);
    } catch (Exception e) {
      LOGGER.error("Failed to create flat collection schema: {}", e.getMessage(), e);
      throw new RuntimeException("Failed to create flat collection schema", e);
    }
  }

  protected static String generateDocId(String prefix) {
    return prefix + "-" + System.currentTimeMillis() + "-" + (int) (Math.random() * 10000);
  }

  protected static String getKeyString(String docId) {
    return new SingleValueKey(DEFAULT_TENANT, docId).toString();
  }

  protected Query buildQueryById(String docId) {
    return Query.builder()
        .setFilter(
            RelationalExpression.of(
                IdentifierExpression.of("id"),
                RelationalOperator.EQ,
                ConstantExpression.of(getKeyString(docId))))
        .build();
  }

  protected Document createTestDocument(String docId) {
    Key key = new SingleValueKey(DEFAULT_TENANT, docId);
    String keyStr = key.toString();

    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put("id", keyStr);
    objectNode.put("item", "TestItem");
    objectNode.put("price", 100);
    objectNode.put("quantity", 50);
    objectNode.put("in_stock", true);
    objectNode.put("big_number", 1000000000000L);
    objectNode.put("rating", 3.5);
    objectNode.put("weight", 50.0);
    objectNode.putArray("tags").add("tag1").add("tag2");
    objectNode.putArray("numbers").add(1).add(2).add(3);
    ObjectNode props = OBJECT_MAPPER.createObjectNode();
    props.put("brand", "TestBrand");
    props.put("size", "M");
    props.put("count", 10);
    props.putArray("colors").add("red").add("blue");
    objectNode.set("props", props);
    ObjectNode sales = OBJECT_MAPPER.createObjectNode();
    sales.put("total", 200);
    sales.put("count", 10);
    objectNode.set("sales", sales);

    return new JSONDocument(objectNode);
  }

  protected Key createKey(String docId) {
    return new SingleValueKey(DEFAULT_TENANT, docId);
  }

  protected static void clearTable(String tableName) {
    PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
    String deleteSQL = String.format("DELETE FROM \"%s\"", tableName);
    try (Connection connection = pgDatastore.getPostgresClient();
        PreparedStatement statement = connection.prepareStatement(deleteSQL)) {
      statement.executeUpdate();
    } catch (Exception e) {
      LOGGER.error("Failed to clear table {}: {}", tableName, e.getMessage(), e);
    }
  }

  protected void insertTestDocument(String docId, Collection collection) throws IOException {
    Key key = createKey(docId);
    Document document = createTestDocument(docId);
    collection.upsert(key, document);
  }

  /** Provides all MissingColumnStrategy values for parameterized tests */
  protected static class MissingColumnStrategyProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return Stream.of(
          Arguments.of(MissingColumnStrategy.SKIP),
          Arguments.of(MissingColumnStrategy.THROW),
          Arguments.of(MissingColumnStrategy.IGNORE_DOCUMENT));
    }
  }
}
