package org.hypertrace.core.documentstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.hypertrace.core.documentstore.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

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

  protected static final String FLAT_COLLECTION_SCHEMA_SQL =
      "CREATE TABLE \"%s\" ("
          + "\"id\" TEXT PRIMARY KEY,"
          + "\"item\" TEXT,"
          + "\"price\" INTEGER,"
          + "\"quantity\" INTEGER,"
          + "\"date\" TIMESTAMPTZ,"
          + "\"in_stock\" BOOLEAN,"
          + "\"tags\" TEXT[],"
          + "\"categoryTags\" TEXT[],"
          + "\"props\" JSONB,"
          + "\"sales\" JSONB,"
          + "\"numbers\" INTEGER[],"
          + "\"scores\" DOUBLE PRECISION[],"
          + "\"flags\" BOOLEAN[],"
          + "\"big_number\" BIGINT,"
          + "\"rating\" REAL,"
          + "\"created_date\" DATE,"
          + "\"weight\" DOUBLE PRECISION"
          + ");";

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
    String createTableSQL = String.format(FLAT_COLLECTION_SCHEMA_SQL, tableName);

    try (Connection connection = pgDatastore.getPostgresClient();
        PreparedStatement statement = connection.prepareStatement(createTableSQL)) {
      statement.execute();
      LOGGER.info("Created flat collection table: {}", tableName);
    } catch (Exception e) {
      LOGGER.error("Failed to create flat collection schema: {}", e.getMessage(), e);
      throw new RuntimeException("Failed to create flat collection schema", e);
    }
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

  protected Document createTestDocument(String docId) throws IOException {
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
}
