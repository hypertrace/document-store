package org.hypertrace.core.documentstore.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.documentstore.commons.SchemaRegistry;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class PostgresSchemaRegistryIntegrationTest {

  private static final String TABLE_NAME = "myTestFlat";

  private static GenericContainer<?> postgres;
  private static PostgresDatastore datastore;
  private static SchemaRegistry<PostgresColumnMetadata> registry;

  @BeforeAll
  static void init() throws Exception {
    postgres =
        new GenericContainer<>(DockerImageName.parse("postgres:13.1"))
            .withEnv("POSTGRES_PASSWORD", "postgres")
            .withEnv("POSTGRES_USER", "postgres")
            .withExposedPorts(5432)
            .waitingFor(Wait.forListeningPort());
    postgres.start();

    String connectionUrl =
        String.format("jdbc:postgresql://localhost:%s/", postgres.getMappedPort(5432));

    Map<String, String> postgresConfig = new HashMap<>();
    postgresConfig.put("url", connectionUrl);
    postgresConfig.put("user", "postgres");
    postgresConfig.put("password", "postgres");
    Config config = ConfigFactory.parseMap(postgresConfig);

    datastore = (PostgresDatastore) DatastoreProvider.getDatastore("Postgres", config);

    createFlatTable();

    registry = datastore.getSchemaRegistry();
  }

  private static void createFlatTable() throws Exception {
    String createTableSQL =
        String.format(
            "CREATE TABLE IF NOT EXISTS \"%s\" ("
                + "\"_id\" INTEGER PRIMARY KEY,"
                + "\"item\" TEXT,"
                + "\"price\" INTEGER,"
                + "\"quantity\" BIGINT,"
                + "\"rating\" REAL,"
                + "\"score\" DOUBLE PRECISION,"
                + "\"date\" TIMESTAMPTZ,"
                + "\"created_date\" DATE,"
                + "\"in_stock\" BOOLEAN,"
                + "\"tags\" TEXT[],"
                + "\"props\" JSONB"
                + ");",
            TABLE_NAME);

    try (Connection connection = datastore.getPostgresClient();
        PreparedStatement statement = connection.prepareStatement(createTableSQL)) {
      statement.execute();
      System.out.println("Created flat table: " + TABLE_NAME);
    }
  }

  @BeforeEach
  void setUp() {
    registry.invalidate(TABLE_NAME);
  }

  @AfterAll
  static void shutdown() {
    if (postgres != null) {
      postgres.stop();
    }
  }

  @Test
  void getSchemaReturnsAllColumns() {
    Map<String, PostgresColumnMetadata> schema = registry.getSchema(TABLE_NAME);

    assertNotNull(schema);
    assertEquals(11, schema.size());
    assertTrue(schema.containsKey("_id"));
    assertTrue(schema.containsKey("item"));
    assertTrue(schema.containsKey("price"));
    assertTrue(schema.containsKey("quantity"));
    assertTrue(schema.containsKey("rating"));
    assertTrue(schema.containsKey("score"));
    assertTrue(schema.containsKey("date"));
    assertTrue(schema.containsKey("created_date"));
    assertTrue(schema.containsKey("in_stock"));
    assertTrue(schema.containsKey("tags"));
    assertTrue(schema.containsKey("props"));
  }

  @Test
  void getSchemaReturnsCorrectIntegerMapping() {
    Map<String, PostgresColumnMetadata> schema = registry.getSchema(TABLE_NAME);

    PostgresColumnMetadata idMeta = schema.get("_id");
    assertEquals("_id", idMeta.getName());
    assertEquals(DataType.INTEGER, idMeta.getCanonicalType());
    assertEquals(PostgresDataType.INTEGER, idMeta.getPostgresType());
    assertFalse(idMeta.isNullable());

    PostgresColumnMetadata priceMeta = schema.get("price");
    assertEquals(DataType.INTEGER, priceMeta.getCanonicalType());
    assertEquals(PostgresDataType.INTEGER, priceMeta.getPostgresType());
    assertTrue(priceMeta.isNullable());
  }

  @Test
  void getSchemaReturnsCorrectBigintMapping() {
    Map<String, PostgresColumnMetadata> schema = registry.getSchema(TABLE_NAME);

    PostgresColumnMetadata quantityMeta = schema.get("quantity");
    assertEquals(DataType.LONG, quantityMeta.getCanonicalType());
    assertEquals(PostgresDataType.BIGINT, quantityMeta.getPostgresType());
  }

  @Test
  void getSchemaReturnsCorrectFloatMapping() {
    Map<String, PostgresColumnMetadata> schema = registry.getSchema(TABLE_NAME);

    PostgresColumnMetadata ratingMeta = schema.get("rating");
    assertEquals(DataType.FLOAT, ratingMeta.getCanonicalType());
    assertEquals(PostgresDataType.REAL, ratingMeta.getPostgresType());
  }

  @Test
  void getSchemaReturnsCorrectDoubleMapping() {
    Map<String, PostgresColumnMetadata> schema = registry.getSchema(TABLE_NAME);

    PostgresColumnMetadata scoreMeta = schema.get("score");
    assertEquals(DataType.DOUBLE, scoreMeta.getCanonicalType());
    assertEquals(PostgresDataType.DOUBLE_PRECISION, scoreMeta.getPostgresType());
  }

  @Test
  void getSchemaReturnsCorrectTextMapping() {
    Map<String, PostgresColumnMetadata> schema = registry.getSchema(TABLE_NAME);

    PostgresColumnMetadata itemMeta = schema.get("item");
    assertEquals(DataType.STRING, itemMeta.getCanonicalType());
    assertEquals(PostgresDataType.TEXT, itemMeta.getPostgresType());
  }

  @Test
  void getSchemaReturnsCorrectBooleanMapping() {
    Map<String, PostgresColumnMetadata> schema = registry.getSchema(TABLE_NAME);

    PostgresColumnMetadata inStockMeta = schema.get("in_stock");
    assertEquals(DataType.BOOLEAN, inStockMeta.getCanonicalType());
    assertEquals(PostgresDataType.BOOLEAN, inStockMeta.getPostgresType());
  }

  @Test
  void getSchemaReturnsCorrectTimestamptzMapping() {
    Map<String, PostgresColumnMetadata> schema = registry.getSchema(TABLE_NAME);

    PostgresColumnMetadata dateMeta = schema.get("date");
    assertEquals(DataType.TIMESTAMPTZ, dateMeta.getCanonicalType());
    assertEquals(PostgresDataType.TIMESTAMPTZ, dateMeta.getPostgresType());
  }

  @Test
  void getSchemaReturnsCorrectDateMapping() {
    Map<String, PostgresColumnMetadata> schema = registry.getSchema(TABLE_NAME);

    PostgresColumnMetadata createdDateMeta = schema.get("created_date");
    assertEquals(DataType.DATE, createdDateMeta.getCanonicalType());
    assertEquals(PostgresDataType.DATE, createdDateMeta.getPostgresType());
  }

  @Test
  void getSchemaReturnsCorrectJsonbMapping() {
    Map<String, PostgresColumnMetadata> schema = registry.getSchema(TABLE_NAME);

    PostgresColumnMetadata propsMeta = schema.get("props");
    assertEquals(DataType.JSON, propsMeta.getCanonicalType());
    assertEquals(PostgresDataType.JSONB, propsMeta.getPostgresType());
  }

  @Test
  void getColumnOrRefreshReturnsExistingColumn() {
    Optional<PostgresColumnMetadata> result = registry.getColumnOrRefresh(TABLE_NAME, "item");

    assertTrue(result.isPresent());
    assertEquals("item", result.get().getName());
    assertEquals(DataType.STRING, result.get().getCanonicalType());
  }

  @Test
  void getColumnOrRefreshReturnsEmptyForNonExistentColumn() {
    Optional<PostgresColumnMetadata> result =
        registry.getColumnOrRefresh(TABLE_NAME, "nonexistent_column");

    assertFalse(result.isPresent());
  }

  @Test
  void getColumnOrRefreshFindsNewlyAddedColumnAfterInvalidation() throws Exception {
    // First, verify the new column doesn't exist
    Optional<PostgresColumnMetadata> before = registry.getColumnOrRefresh(TABLE_NAME, "new_column");
    assertFalse(before.isPresent());

    // Add a new column to the table
    try (Connection connection = datastore.getPostgresClient();
        PreparedStatement statement =
            connection.prepareStatement(
                String.format("ALTER TABLE \"%s\" ADD COLUMN \"new_column\" TEXT", TABLE_NAME))) {
      statement.execute();
    }

    // Invalidate cache to force reload
    registry.invalidate(TABLE_NAME);

    // Now the registry should find the new column after reload
    Optional<PostgresColumnMetadata> after = registry.getColumnOrRefresh(TABLE_NAME, "new_column");
    assertTrue(after.isPresent());
    assertEquals("new_column", after.get().getName());
    assertEquals(DataType.STRING, after.get().getCanonicalType());

    // Cleanup: drop the column
    try (Connection connection = datastore.getPostgresClient();
        PreparedStatement statement =
            connection.prepareStatement(
                String.format("ALTER TABLE \"%s\" DROP COLUMN \"new_column\"", TABLE_NAME))) {
      statement.execute();
    }
  }

  @Test
  void cacheReturnsSameInstanceOnSubsequentCalls() {
    Map<String, PostgresColumnMetadata> schema1 = registry.getSchema(TABLE_NAME);
    Map<String, PostgresColumnMetadata> schema2 = registry.getSchema(TABLE_NAME);

    // Should be the same cached instance
    assertTrue(schema1 == schema2);
  }

  @Test
  void invalidateCausesReload() {
    Map<String, PostgresColumnMetadata> schema1 = registry.getSchema(TABLE_NAME);

    registry.invalidate(TABLE_NAME);

    Map<String, PostgresColumnMetadata> schema2 = registry.getSchema(TABLE_NAME);

    // Should be different instances after invalidation
    assertFalse(schema1 == schema2);
    // But same content
    assertEquals(schema1.keySet(), schema2.keySet());
  }
}
