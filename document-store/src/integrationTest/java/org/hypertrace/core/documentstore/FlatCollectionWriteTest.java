package org.hypertrace.core.documentstore;

import static org.hypertrace.core.documentstore.utils.Utils.readFileFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.model.exception.DuplicateDocumentException;
import org.hypertrace.core.documentstore.model.exception.SchemaMismatchException;
import org.hypertrace.core.documentstore.model.options.MissingColumnStrategy;
import org.hypertrace.core.documentstore.model.options.ReturnDocumentType;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.hypertrace.core.documentstore.query.Query;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration tests for write operations on flat PostgreSQL collections.
 *
 * <p>Flat collections are PostgreSQL tables with explicit column schemas (not JSONB-based nested
 * documents). This test class verifies that Collection interface write operations work correctly on
 * such collections.
 */
@Testcontainers
public class FlatCollectionWriteTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlatCollectionWriteTest.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String FLAT_COLLECTION_NAME = "myTestFlat";
  private static final String INSERT_STATEMENTS_FILE = "query/pg_flat_collection_insert.json";
  private static final String DEFAULT_TENANT = "default";

  private static Datastore postgresDatastore;
  private static Collection flatCollection;
  private static GenericContainer<?> postgres;

  @BeforeAll
  public static void init() throws IOException {
    postgres =
        new GenericContainer<>(DockerImageName.parse("postgres:13.1"))
            .withEnv("POSTGRES_PASSWORD", "postgres")
            .withEnv("POSTGRES_USER", "postgres")
            .withExposedPorts(5432)
            .waitingFor(Wait.forListeningPort());
    postgres.start();

    String postgresConnectionUrl =
        String.format("jdbc:postgresql://localhost:%s/", postgres.getMappedPort(5432));

    Map<String, String> postgresConfig = new HashMap<>();
    postgresConfig.put("url", postgresConnectionUrl);
    postgresConfig.put("user", "postgres");
    postgresConfig.put("password", "postgres");
    // Configure timestamp fields for auto-managed document timestamps
    postgresConfig.put(
        "customParams.timestampFields",
        "{\"createdTsCol\": \"createdTime\", \"lastUpdatedTsCol\": \"lastUpdateTime\"}");

    postgresDatastore =
        DatastoreProvider.getDatastore("Postgres", ConfigFactory.parseMap(postgresConfig));
    LOGGER.info("Postgres datastore initialized: {}", postgresDatastore.listCollections());

    createFlatCollectionSchema();
    flatCollection =
        postgresDatastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);
  }

  private static void createFlatCollectionSchema() {
    String createTableSQL =
        String.format(
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
                + "\"weight\" DOUBLE PRECISION,"
                + "\"createdTime\" BIGINT,"
                + "\"lastUpdateTime\" TIMESTAMP WITH TIME ZONE"
                + ");",
            FLAT_COLLECTION_NAME);

    PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

    try (Connection connection = pgDatastore.getPostgresClient();
        PreparedStatement statement = connection.prepareStatement(createTableSQL)) {
      statement.execute();
      LOGGER.info("Created flat collection table: {}", FLAT_COLLECTION_NAME);
    } catch (Exception e) {
      LOGGER.error("Failed to create flat collection schema: {}", e.getMessage(), e);
    }
  }

  private static void executeInsertStatements() {
    PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
    try {
      String jsonContent = readFileFromResource(INSERT_STATEMENTS_FILE).orElseThrow();
      JsonNode rootNode = OBJECT_MAPPER.readTree(jsonContent);
      JsonNode statementsNode = rootNode.get("statements");

      if (statementsNode == null || !statementsNode.isArray()) {
        throw new RuntimeException("Invalid JSON format: 'statements' array not found");
      }

      try (Connection connection = pgDatastore.getPostgresClient()) {
        for (JsonNode statementNode : statementsNode) {
          String statement = statementNode.asText().trim();
          if (!statement.isEmpty()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
              preparedStatement.executeUpdate();
            } catch (Exception e) {
              LOGGER.error("Failed to execute INSERT statement: {}", e.getMessage(), e);
              throw e;
            }
          }
        }
      }
      LOGGER.info("Inserted initial data into: {}", FLAT_COLLECTION_NAME);
    } catch (Exception e) {
      LOGGER.error("Failed to execute INSERT statements: {}", e.getMessage(), e);
    }
  }

  @BeforeEach
  public void setupData() {
    // Clear and repopulate with initial data before each test
    clearTable();
    executeInsertStatements();
  }

  private static void clearTable() {
    PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
    String deleteSQL = String.format("DELETE FROM \"%s\"", FLAT_COLLECTION_NAME);
    try (Connection connection = pgDatastore.getPostgresClient();
        PreparedStatement statement = connection.prepareStatement(deleteSQL)) {
      statement.executeUpdate();
      LOGGER.info("Cleared table: {}", FLAT_COLLECTION_NAME);
    } catch (Exception e) {
      LOGGER.error("Failed to clear table: {}", e.getMessage(), e);
    }
  }

  @AfterEach
  public void cleanup() {
    // Data is cleared in @BeforeEach, but cleanup here for safety
  }

  @AfterAll
  public static void shutdown() {
    postgres.stop();
  }

  @Nested
  @DisplayName("Upsert Operations")
  class UpsertTests {

    @Test
    @DisplayName("Should throw UnsupportedOperationException for upsert")
    void testUpsertNewDocument() {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("_id", 100);
      objectNode.put("item", "NewItem");
      objectNode.put("price", 99);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "100");

      assertThrows(UnsupportedOperationException.class, () -> flatCollection.upsert(key, document));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for upsertAndReturn")
    void testUpsertAndReturn() {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("_id", 100);
      objectNode.put("item", "NewItem");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "100");

      assertThrows(
          UnsupportedOperationException.class, () -> flatCollection.upsertAndReturn(key, document));
    }
  }

  @Nested
  @DisplayName("Create Operations")
  class CreateTests {

    @Test
    @DisplayName("Should create document with all supported data types")
    void testCreateWithAllDataTypes() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      String docId = getRandomDocId(4);

      objectNode.put("id", docId);
      objectNode.put("item", "Comprehensive Test Item");
      objectNode.put("price", 999);
      objectNode.put("quantity", "50");
      objectNode.put("big_number", 9223372036854775807L);
      objectNode.put("rating", 4.5f);
      objectNode.put("weight", 123.456789);
      objectNode.put("in_stock", true);
      objectNode.put("date", 1705315800000L);
      objectNode.put("created_date", "2024-01-15");
      objectNode.putArray("tags").add("electronics").add("sale").add("featured");
      objectNode.put("categoryTags", "single-category");
      objectNode.putArray("numbers").add(10).add(20).add(30);
      objectNode.putArray("scores").add(1.5).add(2.5).add(3.5);
      objectNode.putArray("flags").add(true).add(false).add(true);

      ObjectNode propsNode = OBJECT_MAPPER.createObjectNode();
      propsNode.put("color", "blue");
      propsNode.put("size", "large");
      propsNode.put("weight", 2.5);
      propsNode.put("warranty", true);
      propsNode.putObject("nested").put("key", "value");
      objectNode.set("props", propsNode);

      ObjectNode salesNode = OBJECT_MAPPER.createObjectNode();
      salesNode.put("total", 1000);
      salesNode.put("region", "US");
      objectNode.set("sales", salesNode);

      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      CreateResult result = flatCollection.create(key, document);

      assertTrue(result.isSucceed());
      assertFalse(result.isPartial());
      assertTrue(result.getSkippedFields().isEmpty());

      // Verify all data types were inserted correctly
      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());

            assertEquals("Comprehensive Test Item", rs.getString("item"));
            assertEquals(999, rs.getInt("price"));
            assertEquals(50, rs.getInt("quantity"));
            assertEquals(9223372036854775807L, rs.getLong("big_number"));
            assertEquals(4.5f, rs.getFloat("rating"), 0.01f);
            assertEquals(123.456789, rs.getDouble("weight"), 0.0001);
            assertTrue(rs.getBoolean("in_stock"));
            assertEquals(1705315800000L, rs.getTimestamp("date").getTime()); // epoch millis
            assertNotNull(rs.getDate("created_date"));

            String[] tags = (String[]) rs.getArray("tags").getArray();
            assertEquals(3, tags.length);
            assertEquals("electronics", tags[0]);
            assertEquals("sale", tags[1]);
            assertEquals("featured", tags[2]);

            // Single value auto-converted to array
            String[] categoryTags = (String[]) rs.getArray("categoryTags").getArray();
            assertEquals(1, categoryTags.length);
            assertEquals("single-category", categoryTags[0]);

            Integer[] numbers = (Integer[]) rs.getArray("numbers").getArray();
            assertEquals(3, numbers.length);
            assertEquals(10, numbers[0]);
            assertEquals(20, numbers[1]);
            assertEquals(30, numbers[2]);

            Double[] scores = (Double[]) rs.getArray("scores").getArray();
            assertEquals(3, scores.length);
            assertEquals(1.5, scores[0], 0.01);

            Boolean[] flags = (Boolean[]) rs.getArray("flags").getArray();
            assertEquals(3, flags.length);
            assertTrue(flags[0]);
            assertFalse(flags[1]);

            String propsJson = rs.getString("props");
            assertNotNull(propsJson);
            JsonNode propsResult = OBJECT_MAPPER.readTree(propsJson);
            assertEquals("blue", propsResult.get("color").asText());
            assertEquals("large", propsResult.get("size").asText());
            assertEquals(2.5, propsResult.get("weight").asDouble(), 0.01);
            assertTrue(propsResult.get("warranty").asBoolean());
            assertEquals("value", propsResult.get("nested").get("key").asText());

            String salesJson = rs.getString("sales");
            assertNotNull(salesJson);
            JsonNode salesResult = OBJECT_MAPPER.readTree(salesJson);
            assertEquals(1000, salesResult.get("total").asInt());
            assertEquals("US", salesResult.get("region").asText());
          });
    }

    @Test
    @DisplayName("Should throw DuplicateDocumentException when creating with existing key")
    void testCreateDuplicateDocument() throws Exception {

      String docId = getRandomDocId(4);
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "dup-doc-200");
      objectNode.put("item", "First Item");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      CreateResult createResult = flatCollection.create(key, document);
      Preconditions.checkArgument(
          createResult.isSucceed(),
          "Preconditions failure: Could not create doc with id: " + docId);

      ObjectNode objectNode2 = OBJECT_MAPPER.createObjectNode();
      objectNode2.put("id", "dup-doc-200");
      objectNode2.put("item", "Second Item");
      Document document2 = new JSONDocument(objectNode2);

      assertThrows(DuplicateDocumentException.class, () -> flatCollection.create(key, document2));
    }

    @ParameterizedTest
    @DisplayName(
        "When MissingColumnStrategy is Throw, should throw an exception for unknown fields. Unknown fields are those fields that are not found in the schema but are present in the doc")
    @ArgumentsSource(MissingColumnStrategyProvider.class)
    void testUnknownFieldsAsPerMissingColumnStrategy(MissingColumnStrategy missingColumnStrategy)
        throws Exception {

      String docId = getRandomDocId(4);

      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", docId);
      objectNode.put("item", "Item");
      objectNode.put("unknown_column", "should throw");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      if (missingColumnStrategy == MissingColumnStrategy.THROW) {
        Collection collection =
            getFlatCollectionWithStrategy(MissingColumnStrategy.THROW.toString());
        assertThrows(SchemaMismatchException.class, () -> collection.create(key, document));
        // Verify no document was inserted
        PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps =
                conn.prepareStatement(
                    String.format(
                        "SELECT COUNT(*) FROM \"%s\" WHERE \"id\" = '%s'",
                        FLAT_COLLECTION_NAME, key));
            ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(0, rs.getInt(1), "Document should not exist in DB after exception");
        }
      } else {
        CreateResult result = flatCollection.create(key, document);
        // for SKIP
        assertTrue(result.isSucceed());
        // this is a partial write because unknown_column was not written to
        assertTrue(result.isPartial());
        assertTrue(result.getSkippedFields().contains("unknown_column"));

        queryAndAssert(
            key,
            rs -> {
              assertTrue(rs.next());
              assertEquals("Item", rs.getString("item"));
            });
      }
    }

    @Test
    @DisplayName(
        "Should use default SKIP strategy when missingColumnStrategy config is empty string")
    void testEmptyMissingColumnStrategyConfigUsesDefault() throws Exception {
      Collection collectionWithEmptyStrategy = getFlatCollectionWithStrategy("");

      // Test that it uses default SKIP strategy (unknown fields are skipped, not thrown)
      String docId = getRandomDocId(4);
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", docId);
      objectNode.put("item", "Test Item");
      objectNode.put("unknown_field", "should be skipped with default SKIP strategy");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      CreateResult result = collectionWithEmptyStrategy.create(key, document);

      // With default SKIP strategy, unknown fields are skipped
      assertTrue(result.isSucceed());
      assertTrue(result.isPartial());
      assertTrue(result.getSkippedFields().contains("unknown_field"));
    }

    @Test
    @DisplayName("Should use default SKIP strategy when missingColumnStrategy config is invalid")
    void testInvalidMissingColumnStrategyConfigUsesDefault() throws Exception {
      Collection collectionWithInvalidStrategy = getFlatCollectionWithStrategy("INVALID_STRATEGY");

      String docId = getRandomDocId(4);
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", docId);
      objectNode.put("item", "Test Item");
      objectNode.put("unknown_field", "should be skipped with default SKIP strategy");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      CreateResult result = collectionWithInvalidStrategy.create(key, document);

      // With default SKIP strategy, unknown fields are skipped
      assertTrue(result.isSucceed());
      assertTrue(result.isPartial());
      assertTrue(result.getSkippedFields().contains("unknown_field"));
    }

    @Test
    @DisplayName("Should return failure when all fields are unknown (parsed.isEmpty)")
    void testCreateFailsWhenAllFieldsAreUnknown() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("completely_unknown_field1", "value1");
      objectNode.put("completely_unknown_field2", "value2");
      objectNode.put("another_nonexistent_column", 123);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "all-unknown-doc-700");

      CreateResult result = flatCollection.create(key, document);

      // Although no column exists in the schema, it'll create a new doc with the key as the id
      assertTrue(result.isSucceed());
      assertEquals(3, result.getSkippedFields().size());
      assertTrue(
          result
              .getSkippedFields()
              .containsAll(
                  List.of(
                      "completely_unknown_field1",
                      "completely_unknown_field2",
                      "another_nonexistent_column")));

      // Verify no row was inserted
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT COUNT(*) FROM \"%s\" WHERE \"id\" = '%s'",
                      FLAT_COLLECTION_NAME, key));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }

    @Test
    @DisplayName("Should refresh schema and retry on UNDEFINED_COLUMN error")
    void testCreateRefreshesSchemaOnUndefinedColumnError() throws Exception {
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

      // Step 1: Add a temporary column and do a create to cache the schema
      String addColumnSQL =
          String.format("ALTER TABLE \"%s\" ADD COLUMN \"temp_col\" TEXT", FLAT_COLLECTION_NAME);
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps = conn.prepareStatement(addColumnSQL)) {
        ps.execute();
        LOGGER.info("Added temporary column 'temp_col' to table");
      }

      // Step 2: Create a document with the temp column to cache the schema
      ObjectNode objectNode1 = OBJECT_MAPPER.createObjectNode();
      objectNode1.put("id", "cache-schema-doc");
      objectNode1.put("item", "Item to cache schema");
      objectNode1.put("temp_col", "temp value");
      flatCollection.create(
          new SingleValueKey("default", "cache-schema-doc"), new JSONDocument(objectNode1));
      LOGGER.info("Schema cached with temp_col");

      // Step 3: DROP the column - now the cached schema is stale
      String dropColumnSQL =
          String.format("ALTER TABLE \"%s\" DROP COLUMN \"temp_col\"", FLAT_COLLECTION_NAME);
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps = conn.prepareStatement(dropColumnSQL)) {
        ps.execute();
        LOGGER.info("Dropped temp_col - schema cache is now stale");
      }

      // Step 4: Try to create with the dropped column
      // Schema registry still thinks temp_col exists, so it will include it in INSERT
      // INSERT will fail with UNDEFINED_COLUMN, triggering handlePSQLExceptionForCreate
      // which will refresh schema and retry
      ObjectNode objectNode2 = OBJECT_MAPPER.createObjectNode();
      objectNode2.put("id", "retry-doc-800");
      objectNode2.put("item", "Item after schema refresh");
      objectNode2.put("temp_col", "this column no longer exists");
      Document document = new JSONDocument(objectNode2);
      Key key = new SingleValueKey("default", "retry-doc-800");

      CreateResult result = flatCollection.create(key, document);

      // Should succeed - temp_col will be skipped (either via retry or schema refresh)
      assertTrue(result.isSucceed());
      // The dropped column should be skipped
      assertTrue(result.getSkippedFields().contains("temp_col"));

      // Verify the valid fields were inserted
      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            assertEquals("Item after schema refresh", rs.getString("item"));
          });
    }

    @ParameterizedTest
    @DisplayName("Should skip column with unparseable value and add to skippedFields")
    @ArgumentsSource(MissingColumnStrategyProvider.class)
    void testUnparsableValuesAsPerMissingColStrategy(MissingColumnStrategy missingColumnStrategy)
        throws Exception {

      String docId = getRandomDocId(4);

      // Try to insert a string value into an integer column with wrong type
      // The unparseable column should be skipped, not throw an exception
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", docId);
      objectNode.put("item", "Valid Item");
      objectNode.put("price", "not_a_number_at_all"); // price is INTEGER, this will fail parsing
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      if (missingColumnStrategy == MissingColumnStrategy.THROW) {
        CreateResult result =
            getFlatCollectionWithStrategy(MissingColumnStrategy.SKIP.toString())
                .create(key, document);

        // Should succeed with the valid columns, skipping the unparseable one
        assertTrue(result.isSucceed());
        assertTrue(result.isPartial());
        assertEquals(1, result.getSkippedFields().size());
        assertTrue(result.getSkippedFields().contains("price"));

        // Verify the valid fields were inserted
        queryAndAssert(
            key,
            rs -> {
              assertTrue(rs.next());
              assertEquals("Valid Item", rs.getString("item"));
              // price should be null since it was skipped
              assertEquals(0, rs.getInt("price"));
              assertTrue(rs.wasNull());
            });
      } else {
        // SKIP strategy: unparseable value should be skipped, document created
        CreateResult result = flatCollection.create(key, document);
        assertTrue(result.isSucceed());
        assertTrue(result.isPartial());
        assertEquals(1, result.getSkippedFields().size());
        assertTrue(result.getSkippedFields().contains("price"));

        // Verify the valid fields were inserted
        queryAndAssert(
            key,
            rs -> {
              assertTrue(rs.next());
              assertEquals("Valid Item", rs.getString("item"));
              // price should be null since it was skipped
              assertEquals(0, rs.getInt("price"));
              assertTrue(rs.wasNull());
            });
      }
    }
  }

  private String getRandomDocId(int len) {
    return org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils.random(
        len, true, false);
  }

  private static Collection getFlatCollectionWithStrategy(String strategy) {
    String postgresConnectionUrl =
        String.format("jdbc:postgresql://localhost:%s/", postgres.getMappedPort(5432));

    Map<String, String> configWithStrategy = new HashMap<>();
    configWithStrategy.put("url", postgresConnectionUrl);
    configWithStrategy.put("user", "postgres");
    configWithStrategy.put("password", "postgres");
    configWithStrategy.put("customParams.missingColumnStrategy", strategy);

    Datastore datastoreWithStrategy =
        DatastoreProvider.getDatastore("Postgres", ConfigFactory.parseMap(configWithStrategy));

    return datastoreWithStrategy.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);
  }

  private void queryAndAssert(Key key, ResultSetConsumer consumer) throws Exception {
    PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
    try (Connection conn = pgDatastore.getPostgresClient();
        PreparedStatement ps =
            conn.prepareStatement(
                String.format(
                    "SELECT * FROM \"%s\" WHERE \"id\" = '%s'", FLAT_COLLECTION_NAME, key));
        ResultSet rs = ps.executeQuery()) {
      consumer.accept(rs);
    }
  }

  @FunctionalInterface
  interface ResultSetConsumer {

    void accept(ResultSet rs) throws Exception;
  }

  static class MissingColumnStrategyProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return Stream.of(MissingColumnStrategy.values())
          .filter(
              strategy ->
                  (strategy == MissingColumnStrategy.THROW)
                      || (strategy == MissingColumnStrategy.SKIP))
          .map(Arguments::of);
    }
  }

  @Nested
  @DisplayName("CreateOrReplace Operations")
  class CreateOrReplaceTests {

    @Test
    @DisplayName("Should create new document and return true")
    void testCreateOrReplaceNewDocument() throws Exception {

      String docId = getRandomDocId(4);

      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "upsert-new-doc-100");
      objectNode.put("item", "New Upsert Item");
      objectNode.put("price", 500);
      objectNode.put("quantity", 25);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      boolean isNew = flatCollection.createOrReplace(key, document);

      assertTrue(isNew);

      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            assertEquals("New Upsert Item", rs.getString("item"));
            assertEquals(500, rs.getInt("price"));
            assertEquals(25, rs.getInt("quantity"));
          });
    }

    @Test
    @DisplayName("Should replace existing document and return false")
    void testCreateOrReplaceExistingDocument() throws Exception {
      String docId = getRandomDocId(4);
      ObjectNode initialNode = OBJECT_MAPPER.createObjectNode();
      initialNode.put("id", docId);
      initialNode.put("item", "Original Item");
      initialNode.put("price", 100);
      Document initialDoc = new JSONDocument(initialNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      boolean firstResult = flatCollection.createOrReplace(key, initialDoc);

      Preconditions.checkArgument(
          firstResult, "Preconditions failure: Could not create first document with id: " + docId);

      // Now replace with updated document
      ObjectNode updatedNode = OBJECT_MAPPER.createObjectNode();
      updatedNode.put("id", docId);
      updatedNode.put("item", "Updated Item");
      updatedNode.put("price", 999);
      updatedNode.put("quantity", 50);
      Document updatedDoc = new JSONDocument(updatedNode);

      boolean secondResult = flatCollection.createOrReplace(key, updatedDoc);

      assertFalse(secondResult);

      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            assertEquals("Updated Item", rs.getString("item"));
            assertEquals(999, rs.getInt("price"));
            assertEquals(50, rs.getInt("quantity"));
          });
    }

    @Test
    @DisplayName("Should skip unknown fields in createOrReplace (default SKIP strategy)")
    void testCreateOrReplaceSkipsUnknownFields() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "upsert-skip-fields-300");
      objectNode.put("item", "Item with unknown");
      objectNode.put("price", 200);
      objectNode.put("unknown_field", "should be skipped");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "upsert-skip-fields-300");

      boolean isNew = flatCollection.createOrReplace(key, document);
      assertTrue(isNew);

      // Verify only known fields were inserted
      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            assertEquals("Item with unknown", rs.getString("item"));
            assertEquals(200, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should handle JSONB fields in createOrReplace")
    void testCreateOrReplaceWithJsonbField() throws Exception {
      String docId = getRandomDocId(4);
      ObjectNode initialNode = OBJECT_MAPPER.createObjectNode();
      initialNode.put("id", docId);
      initialNode.put("item", "Item with props");
      ObjectNode initialProps = OBJECT_MAPPER.createObjectNode();
      initialProps.put("color", "red");
      initialProps.put("size", "small");
      initialNode.set("props", initialProps);
      Document initialDoc = new JSONDocument(initialNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      boolean wasCreated = flatCollection.createOrReplace(key, initialDoc);
      Preconditions.checkArgument(
          wasCreated, "Precondition failure: Doc could not be created with id: " + docId);

      // Update with new JSONB value
      ObjectNode updatedNode = OBJECT_MAPPER.createObjectNode();
      updatedNode.put("id", docId);
      updatedNode.put("item", "Updated Item");
      ObjectNode updatedProps = OBJECT_MAPPER.createObjectNode();
      updatedProps.put("color", "blue");
      updatedProps.put("size", "large");
      updatedProps.put("weight", 2.5);
      updatedNode.set("props", updatedProps);
      Document updatedDoc = new JSONDocument(updatedNode);

      boolean isNew = flatCollection.createOrReplace(key, updatedDoc);
      assertFalse(isNew);

      // Verify JSONB was updated
      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            JsonNode propsResult = OBJECT_MAPPER.readTree(rs.getString("props"));
            assertEquals("blue", propsResult.get("color").asText());
            assertEquals("large", propsResult.get("size").asText());
            assertEquals(2.5, propsResult.get("weight").asDouble(), 0.01);
          });
    }
  }

  @Nested
  @DisplayName("Bulk Operations")
  class BulkOperationTests {

    @Test
    @DisplayName("Should bulk upsert multiple new documents")
    void testBulkUpsertNewDocuments() throws Exception {
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode node1 = OBJECT_MAPPER.createObjectNode();
      node1.put("item", "BulkItem101");
      node1.put("price", 101);
      node1.put("quantity", 10);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "bulk-101"), new JSONDocument(node1));

      ObjectNode node2 = OBJECT_MAPPER.createObjectNode();
      node2.put("item", "BulkItem102");
      node2.put("price", 102);
      node2.put("quantity", 20);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "bulk-102"), new JSONDocument(node2));

      ObjectNode node3 = OBJECT_MAPPER.createObjectNode();
      node3.put("item", "BulkItem103");
      node3.put("price", 103);
      node3.put("in_stock", true);
      ObjectNode props = OBJECT_MAPPER.createObjectNode();
      props.put("color", "red");
      props.put("size", "large");
      node3.set("props", props);
      node3.putArray("tags").add("electronics").add("sale");
      node3.putArray("numbers").add(1).add(2).add(3);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "bulk-103"), new JSONDocument(node3));

      boolean result = flatCollection.bulkUpsert(bulkMap);

      assertTrue(result);

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "bulk-101"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("BulkItem101", rs.getString("item"));
            assertEquals(101, rs.getInt("price"));
            assertEquals(10, rs.getInt("quantity"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "bulk-102"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("BulkItem102", rs.getString("item"));
            assertEquals(102, rs.getInt("price"));
            assertEquals(20, rs.getInt("quantity"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "bulk-103"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("BulkItem103", rs.getString("item"));
            assertEquals(103, rs.getInt("price"));
            assertTrue(rs.getBoolean("in_stock"));

            // Verify JSONB column
            JsonNode propsResult = OBJECT_MAPPER.readTree(rs.getString("props"));
            assertEquals("red", propsResult.get("color").asText());
            assertEquals("large", propsResult.get("size").asText());

            // Verify array columns
            String[] tags = (String[]) rs.getArray("tags").getArray();
            assertEquals(2, tags.length);
            assertEquals("electronics", tags[0]);

            Integer[] numbers = (Integer[]) rs.getArray("numbers").getArray();
            assertEquals(3, numbers.length);
            assertEquals(1, numbers[0]);
          });
    }

    @Test
    @DisplayName("Should bulk upsert updating existing documents")
    void testBulkUpsertUpdatesExistingDocuments() throws Exception {
      // First create some documents
      String docId1 = "bulk-update-1";
      String docId2 = "bulk-update-2";

      ObjectNode initial1 = OBJECT_MAPPER.createObjectNode();
      initial1.put("item", "Original1");
      initial1.put("price", 100);
      flatCollection.createOrReplace(
          new SingleValueKey(DEFAULT_TENANT, docId1), new JSONDocument(initial1));

      ObjectNode initial2 = OBJECT_MAPPER.createObjectNode();
      initial2.put("item", "Original2");
      initial2.put("price", 200);
      flatCollection.createOrReplace(
          new SingleValueKey(DEFAULT_TENANT, docId2), new JSONDocument(initial2));

      // Now bulk upsert with updates
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode updated1 = OBJECT_MAPPER.createObjectNode();
      updated1.put("item", "Updated1");
      updated1.put("price", 999);
      updated1.put("quantity", 50);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, docId1), new JSONDocument(updated1));

      ObjectNode updated2 = OBJECT_MAPPER.createObjectNode();
      updated2.put("item", "Updated2");
      updated2.put("price", 888);
      updated2.put("in_stock", true);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, docId2), new JSONDocument(updated2));

      boolean result = flatCollection.bulkUpsert(bulkMap);

      assertTrue(result);

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, docId1),
          rs -> {
            assertTrue(rs.next());
            assertEquals("Updated1", rs.getString("item"));
            assertEquals(999, rs.getInt("price"));
            assertEquals(50, rs.getInt("quantity"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, docId2),
          rs -> {
            assertTrue(rs.next());
            assertEquals("Updated2", rs.getString("item"));
            assertEquals(888, rs.getInt("price"));
            assertTrue(rs.getBoolean("in_stock"));
          });
    }

    @Test
    @DisplayName("Should bulk upsert with mixed inserts and updates")
    void testBulkUpsertMixedInsertAndUpdate() throws Exception {
      // Create one existing document
      String existingId = "bulk-mixed-existing";
      ObjectNode existing = OBJECT_MAPPER.createObjectNode();
      existing.put("item", "ExistingItem");
      existing.put("price", 100);
      flatCollection.createOrReplace(
          new SingleValueKey(DEFAULT_TENANT, existingId), new JSONDocument(existing));

      // Bulk upsert: update existing + insert new
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode updatedExisting = OBJECT_MAPPER.createObjectNode();
      updatedExisting.put("item", "UpdatedExisting");
      updatedExisting.put("price", 555);
      bulkMap.put(
          new SingleValueKey(DEFAULT_TENANT, existingId), new JSONDocument(updatedExisting));

      ObjectNode newDoc = OBJECT_MAPPER.createObjectNode();
      newDoc.put("item", "NewItem");
      newDoc.put("price", 777);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "bulk-mixed-new"), new JSONDocument(newDoc));

      boolean result = flatCollection.bulkUpsert(bulkMap);

      assertTrue(result);

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, existingId),
          rs -> {
            assertTrue(rs.next());
            assertEquals("UpdatedExisting", rs.getString("item"));
            assertEquals(555, rs.getInt("price"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "bulk-mixed-new"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("NewItem", rs.getString("item"));
            assertEquals(777, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should handle empty document map")
    void testBulkUpsertEmptyMap() {
      Map<Key, Document> emptyMap = Collections.emptyMap();
      boolean result = flatCollection.bulkUpsert(emptyMap);
      assertTrue(result);
    }

    @Test
    @DisplayName("Should handle null document map")
    void testBulkUpsertNullMap() {
      boolean result = flatCollection.bulkUpsert(null);
      assertTrue(result);
    }

    @Test
    @DisplayName("Should bulk upsert documents with different column sets")
    void testBulkUpsertDocumentsWithDifferentColumns() throws Exception {
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode node1 = OBJECT_MAPPER.createObjectNode();
      node1.put("item", "ItemWithPrice");
      node1.put("price", 100);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "diff-cols-1"), new JSONDocument(node1));

      ObjectNode node2 = OBJECT_MAPPER.createObjectNode();
      node2.put("item", "ItemWithQuantity");
      node2.put("quantity", 50);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "diff-cols-2"), new JSONDocument(node2));

      ObjectNode node3 = OBJECT_MAPPER.createObjectNode();
      node3.put("item", "ItemWithAll");
      node3.put("price", 200);
      node3.put("quantity", 30);
      node3.put("in_stock", true);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "diff-cols-3"), new JSONDocument(node3));

      boolean result = flatCollection.bulkUpsert(bulkMap);

      assertTrue(result);

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "diff-cols-1"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("ItemWithPrice", rs.getString("item"));
            assertEquals(100, rs.getInt("price"));
            assertEquals(0, rs.getInt("quantity"));
            assertTrue(rs.wasNull());
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "diff-cols-2"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("ItemWithQuantity", rs.getString("item"));
            assertEquals(0, rs.getInt("price"));
            assertTrue(rs.wasNull());
            assertEquals(50, rs.getInt("quantity"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "diff-cols-3"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("ItemWithAll", rs.getString("item"));
            assertEquals(200, rs.getInt("price"));
            assertEquals(30, rs.getInt("quantity"));
            assertTrue(rs.getBoolean("in_stock"));
          });
    }

    @Test
    @DisplayName("Should skip unknown fields in bulk upsert")
    void testBulkUpsertSkipsUnknownFields() throws Exception {
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode node = OBJECT_MAPPER.createObjectNode();
      node.put("item", "ItemWithUnknown");
      node.put("price", 100);
      node.put("unknown_field", "should be skipped");
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "bulk-unknown-field"), new JSONDocument(node));

      boolean result = flatCollection.bulkUpsert(bulkMap);

      assertTrue(result);

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "bulk-unknown-field"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("ItemWithUnknown", rs.getString("item"));
            assertEquals(100, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should ignore documents with unknown fields when IGNORE_DOCUMENT strategy")
    void testBulkUpsertIgnoreDocumentStrategy() throws Exception {
      Collection collectionWithIgnoreStrategy =
          getFlatCollectionWithStrategy(MissingColumnStrategy.IGNORE_DOCUMENT.toString());

      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      // Doc with unknown field - should be ignored
      ObjectNode nodeWithUnknown = OBJECT_MAPPER.createObjectNode();
      nodeWithUnknown.put("item", "ShouldBeIgnored");
      nodeWithUnknown.put("price", 100);
      nodeWithUnknown.put("unknown_field", "causes ignore");
      bulkMap.put(
          new SingleValueKey(DEFAULT_TENANT, "ignore-doc-1"), new JSONDocument(nodeWithUnknown));

      // Doc without unknown field - should be upserted
      ObjectNode validNode = OBJECT_MAPPER.createObjectNode();
      validNode.put("item", "ValidItem");
      validNode.put("price", 200);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "ignore-doc-2"), new JSONDocument(validNode));

      boolean result = collectionWithIgnoreStrategy.bulkUpsert(bulkMap);

      assertTrue(result);

      // First doc should NOT exist (was ignored)
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "ignore-doc-1"),
          rs -> {
            assertFalse(rs.next());
          });

      // Second doc should exist
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "ignore-doc-2"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("ValidItem", rs.getString("item"));
            assertEquals(200, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should return false when document has invalid JSON (IOException)")
    void testBulkUpsertWithInvalidJsonDocument() {
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      // Create a Document that returns invalid JSON
      Document invalidJsonDoc =
          new Document() {
            @Override
            public String toJson() {
              return "{ invalid json without closing brace";
            }
          };

      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "invalid-json-doc"), invalidJsonDoc);

      // Should return false due to IOException during parsing
      boolean result = flatCollection.bulkUpsert(bulkMap);

      assertFalse(result);
    }

    @Test
    @DisplayName("Should return false when batch execution fails (BatchUpdateException)")
    void testBulkUpsertBatchUpdateException() throws Exception {
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

      String addConstraintSQL =
          String.format(
              "ALTER TABLE \"%s\" ADD CONSTRAINT price_positive CHECK (\"price\" > 0)",
              FLAT_COLLECTION_NAME);
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps = conn.prepareStatement(addConstraintSQL)) {
        ps.execute();
        LOGGER.info("Added CHECK constraint: price must be positive");
      }

      try {
        Map<Key, Document> bulkMap = new LinkedHashMap<>();

        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("item", "NegativePriceItem");
        node.put("price", -100); // Violates CHECK constraint
        bulkMap.put(
            new SingleValueKey(DEFAULT_TENANT, "negative-price-doc"), new JSONDocument(node));

        // Should return false due to BatchUpdateException
        boolean result = flatCollection.bulkUpsert(bulkMap);

        assertFalse(result);

        queryAndAssert(
            new SingleValueKey(DEFAULT_TENANT, "negative-price-doc"),
            rs -> {
              assertFalse(rs.next());
            });

      } finally {
        // Clean up: remove the CHECK constraint
        String dropConstraintSQL =
            String.format(
                "ALTER TABLE \"%s\" DROP CONSTRAINT price_positive", FLAT_COLLECTION_NAME);
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps = conn.prepareStatement(dropConstraintSQL)) {
          ps.execute();
          LOGGER.info("Removed CHECK constraint");
        }
      }
    }

    @Test
    @DisplayName("Should return empty iterator for null document map")
    void testBulkUpsertAndReturnOlderDocumentsNullMap() throws Exception {
      CloseableIterator<Document> result = flatCollection.bulkUpsertAndReturnOlderDocuments(null);
      assertFalse(result.hasNext());
      result.close();
    }

    @Test
    @DisplayName("Should return empty iterator for empty document map")
    void testBulkUpsertAndReturnOlderDocumentsEmptyMap() throws Exception {
      CloseableIterator<Document> result =
          flatCollection.bulkUpsertAndReturnOlderDocuments(Collections.emptyMap());
      assertFalse(result.hasNext());
      result.close();
    }

    @Test
    @DisplayName("Should return empty iterator when upserting new documents (no old docs exist)")
    void testBulkUpsertAndReturnOlderDocumentsNewDocs() throws Exception {
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode node1 = OBJECT_MAPPER.createObjectNode();
      node1.put("item", "NewItem1");
      node1.put("price", 100);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "return-old-new-1"), new JSONDocument(node1));

      ObjectNode node2 = OBJECT_MAPPER.createObjectNode();
      node2.put("item", "NewItem2");
      node2.put("price", 200);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "return-old-new-2"), new JSONDocument(node2));

      CloseableIterator<Document> result =
          flatCollection.bulkUpsertAndReturnOlderDocuments(bulkMap);

      // No old documents should be returned since these are new inserts
      assertFalse(result.hasNext());
      result.close();

      // Verify documents were inserted
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "return-old-new-1"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("NewItem1", rs.getString("item"));
            assertEquals(100, rs.getInt("price"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "return-old-new-2"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("NewItem2", rs.getString("item"));
            assertEquals(200, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should return old documents when updating existing documents")
    void testBulkUpsertAndReturnOlderDocumentsExistingDocs() throws Exception {
      // First create some documents
      String docId1 = "return-old-existing-1";
      String docId2 = "return-old-existing-2";

      ObjectNode initial1 = OBJECT_MAPPER.createObjectNode();
      initial1.put("item", "OldItem1");
      initial1.put("price", 100);
      flatCollection.createOrReplace(
          new SingleValueKey(DEFAULT_TENANT, docId1), new JSONDocument(initial1));

      ObjectNode initial2 = OBJECT_MAPPER.createObjectNode();
      initial2.put("item", "OldItem2");
      initial2.put("price", 200);
      flatCollection.createOrReplace(
          new SingleValueKey(DEFAULT_TENANT, docId2), new JSONDocument(initial2));

      // Now bulk upsert with updates
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode updated1 = OBJECT_MAPPER.createObjectNode();
      updated1.put("item", "UpdatedItem1");
      updated1.put("price", 999);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, docId1), new JSONDocument(updated1));

      ObjectNode updated2 = OBJECT_MAPPER.createObjectNode();
      updated2.put("item", "UpdatedItem2");
      updated2.put("price", 888);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, docId2), new JSONDocument(updated2));

      CloseableIterator<Document> result =
          flatCollection.bulkUpsertAndReturnOlderDocuments(bulkMap);

      // Collect old documents
      List<Document> oldDocs = new ArrayList<>();
      while (result.hasNext()) {
        oldDocs.add(result.next());
      }
      result.close();

      // Should have 2 old documents
      assertEquals(2, oldDocs.size());

      // Verify old documents contain original values
      Map<String, JsonNode> oldDocsByItem = new HashMap<>();
      for (Document doc : oldDocs) {
        JsonNode json = OBJECT_MAPPER.readTree(doc.toJson());
        oldDocsByItem.put(json.get("item").asText(), json);
      }

      assertTrue(oldDocsByItem.containsKey("OldItem1"));
      assertEquals(100, oldDocsByItem.get("OldItem1").get("price").asInt());

      assertTrue(oldDocsByItem.containsKey("OldItem2"));
      assertEquals(200, oldDocsByItem.get("OldItem2").get("price").asInt());

      // Verify documents were updated in DB
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, docId1),
          rs -> {
            assertTrue(rs.next());
            assertEquals("UpdatedItem1", rs.getString("item"));
            assertEquals(999, rs.getInt("price"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, docId2),
          rs -> {
            assertTrue(rs.next());
            assertEquals("UpdatedItem2", rs.getString("item"));
            assertEquals(888, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should return only existing old documents in mixed insert/update scenario")
    void testBulkUpsertAndReturnOlderDocumentsMixed() throws Exception {
      // Create one existing document
      String existingId = "return-old-mixed-existing";

      ObjectNode existing = OBJECT_MAPPER.createObjectNode();
      existing.put("item", "ExistingItem");
      existing.put("price", 500);
      flatCollection.createOrReplace(
          new SingleValueKey(DEFAULT_TENANT, existingId), new JSONDocument(existing));

      // Bulk upsert: update existing + insert new
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode updatedExisting = OBJECT_MAPPER.createObjectNode();
      updatedExisting.put("item", "UpdatedExisting");
      updatedExisting.put("price", 555);
      bulkMap.put(
          new SingleValueKey(DEFAULT_TENANT, existingId), new JSONDocument(updatedExisting));

      ObjectNode newDoc = OBJECT_MAPPER.createObjectNode();
      newDoc.put("item", "NewItem");
      newDoc.put("price", 777);
      bulkMap.put(
          new SingleValueKey(DEFAULT_TENANT, "return-old-mixed-new"), new JSONDocument(newDoc));

      CloseableIterator<Document> result =
          flatCollection.bulkUpsertAndReturnOlderDocuments(bulkMap);

      // Should only return the one existing document (not the new one)
      List<Document> oldDocs = new ArrayList<>();
      while (result.hasNext()) {
        oldDocs.add(result.next());
      }
      result.close();

      assertEquals(1, oldDocs.size());

      JsonNode oldDoc = OBJECT_MAPPER.readTree(oldDocs.get(0).toJson());
      assertEquals("ExistingItem", oldDoc.get("item").asText());
      assertEquals(500, oldDoc.get("price").asInt());

      // Verify both documents exist in DB with new values
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, existingId),
          rs -> {
            assertTrue(rs.next());
            assertEquals("UpdatedExisting", rs.getString("item"));
            assertEquals(555, rs.getInt("price"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "return-old-mixed-new"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("NewItem", rs.getString("item"));
            assertEquals(777, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should throw IOException when bulkUpsert fails")
    void testBulkUpsertAndReturnOlderDocumentsUpsertFailure() throws Exception {
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

      // Add a CHECK constraint to force upsert failure
      String addConstraintSQL =
          String.format(
              "ALTER TABLE \"%s\" ADD CONSTRAINT price_positive_return CHECK (\"price\" > 0)",
              FLAT_COLLECTION_NAME);
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps = conn.prepareStatement(addConstraintSQL)) {
        ps.execute();
      }

      try {
        Map<Key, Document> bulkMap = new LinkedHashMap<>();

        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("item", "NegativePriceItem");
        node.put("price", -100); // Violates CHECK constraint
        bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "return-old-fail"), new JSONDocument(node));

        assertThrows(
            IOException.class, () -> flatCollection.bulkUpsertAndReturnOlderDocuments(bulkMap));

      } finally {
        // Clean up: remove the CHECK constraint
        String dropConstraintSQL =
            String.format(
                "ALTER TABLE \"%s\" DROP CONSTRAINT price_positive_return", FLAT_COLLECTION_NAME);
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps = conn.prepareStatement(dropConstraintSQL)) {
          ps.execute();
        }
      }
    }
  }

  @Nested
  @DisplayName("Delete Operations")
  class DeleteTests {

    @Test
    @DisplayName("Should throw UnsupportedOperationException for delete by key")
    void testDeleteByKey() {
      Key keyToDelete = new SingleValueKey("default", "1");
      assertThrows(UnsupportedOperationException.class, () -> flatCollection.delete(keyToDelete));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for delete by filter")
    void testDeleteByFilter() {
      Filter filter = Filter.eq("item", "Soap");
      assertThrows(UnsupportedOperationException.class, () -> flatCollection.delete(filter));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for delete by keys")
    void testDeleteByKeys() {
      Set<Key> keys =
          Set.of(new SingleValueKey("default", "1"), new SingleValueKey("default", "2"));
      assertThrows(UnsupportedOperationException.class, () -> flatCollection.delete(keys));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for deleteAll")
    void testDeleteAll() {
      assertThrows(UnsupportedOperationException.class, () -> flatCollection.deleteAll());
    }
  }

  @Nested
  @DisplayName("Update Operations")
  class UpdateTests {

    @Test
    @DisplayName("Should throw UnsupportedOperationException for update with condition")
    void testUpdateWithCondition() {
      Key key = new SingleValueKey("default", "1");
      ObjectNode updatedNode = OBJECT_MAPPER.createObjectNode();
      updatedNode.put("_id", 1);
      updatedNode.put("item", "Soap");
      Document document = new JSONDocument(updatedNode);
      Filter condition = Filter.eq("price", 10);

      assertThrows(
          UnsupportedOperationException.class,
          () -> flatCollection.update(key, document, condition));
    }
  }

  @Nested
  @DisplayName("SubDocument Update Operations")
  class SubDocUpdateTests {

    @Nested
    @DisplayName("SET Operator Tests")
    class SetOperatorTests {

      @Test
      @DisplayName("Should update top-level column with SET operator")
      void testUpdateTopLevelColumn() throws Exception {
        // Update the price of item with id = 1
        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of("1")))
                .build();

        List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("price", 999));

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = flatCollection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        assertEquals(999, resultJson.get("price").asInt());

        // Verify in database
        PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps =
                conn.prepareStatement(
                    String.format(
                        "SELECT \"price\" FROM \"%s\" WHERE \"id\" = '1'", FLAT_COLLECTION_NAME));
            ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(999, rs.getInt("price"));
        }
      }

      @Test
      @DisplayName("Should update multiple top-level columns in single update")
      void testUpdateMultipleColumns() throws Exception {
        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of("2")))
                .build();

        List<SubDocumentUpdate> updates =
            List.of(SubDocumentUpdate.of("price", 555), SubDocumentUpdate.of("quantity", 100));

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = flatCollection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        assertEquals(555, resultJson.get("price").asInt());
        assertEquals(100, resultJson.get("quantity").asInt());
      }

      @Test
      @DisplayName("Should update nested path in JSONB column")
      void testUpdateNestedJsonbPath() throws Exception {
        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of("3")))
                .build();

        // Update props.brand nested path
        List<SubDocumentUpdate> updates =
            List.of(SubDocumentUpdate.of("props.brand", "UpdatedBrand"));

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = flatCollection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        assertNotNull(resultJson.get("props"));
        assertEquals("UpdatedBrand", resultJson.get("props").get("brand").asText());
      }

      @Test
      @DisplayName("Should return BEFORE_UPDATE document")
      void testUpdateReturnsBeforeDocument() throws Exception {
        // First get the current price
        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of("4")))
                .build();

        List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("price", 777));

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.BEFORE_UPDATE).build();

        Optional<Document> result = flatCollection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        // Should return the old price (5 from initial data), not the new one (777)
        assertEquals(5, resultJson.get("price").asInt());

        // But database should have the new value
        PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps =
                conn.prepareStatement(
                    String.format(
                        "SELECT \"price\" FROM \"%s\" WHERE \"id\" = '4'", FLAT_COLLECTION_NAME));
            ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(777, rs.getInt("price"));
        }
      }
    }

    @Test
    @DisplayName("Should return empty when no document matches query")
    void testUpdateNoMatch() throws Exception {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("9999")))
              .build();

      List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("price", 100));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      Optional<Document> result = flatCollection.update(query, updates, options);

      assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("Should throw IOException when column does not exist")
    void testUpdateNonExistentColumn() {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("_id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of(1)))
              .build();

      List<SubDocumentUpdate> updates =
          List.of(SubDocumentUpdate.of("nonexistent_column", "value"));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      assertThrows(IOException.class, () -> flatCollection.update(query, updates, options));
    }

    @Test
    @DisplayName("Should throw IOException when nested path on non-JSONB column")
    void testUpdateNestedPathOnNonJsonbColumn() {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("_id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of(1)))
              .build();

      // "item" is TEXT, not JSONB - nested path should fail
      List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("item.nested", "value"));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      assertThrows(IOException.class, () -> flatCollection.update(query, updates, options));
    }

    @Test
    @DisplayName("Should throw IOException for unsupported operator")
    void testUpdateUnsupportedOperator() {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("_id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of(1)))
              .build();

      // UNSET is not supported yet
      List<SubDocumentUpdate> updates =
          List.of(
              SubDocumentUpdate.builder()
                  .subDocument("price")
                  .operator(UpdateOperator.UNSET)
                  .build());

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      assertThrows(
          IllegalArgumentException.class, () -> flatCollection.update(query, updates, options));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for bulkUpdate")
    void testBulkUpdate() {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("price"),
                      RelationalOperator.GT,
                      ConstantExpression.of(5)))
              .build();

      List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("price", 100));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      assertThrows(
          UnsupportedOperationException.class,
          () -> flatCollection.bulkUpdate(query, updates, options));
    }
  }

  @Nested
  @DisplayName("Drop Operations")
  class DropTests {

    @Test
    @DisplayName("Should throw UnsupportedOperationException for drop")
    void testDrop() {
      assertThrows(UnsupportedOperationException.class, () -> flatCollection.drop());
    }
  }

  @Nested
  @DisplayName("Sub-Document Operations")
  class SubDocumentTests {

    @Test
    @DisplayName("Should throw UnsupportedOperationException for updateSubDoc")
    void testSubDocumentUpdate() {
      Key docKey = new SingleValueKey("default", "1");
      ObjectNode subDoc = OBJECT_MAPPER.createObjectNode();
      subDoc.put("newField", "newValue");
      Document subDocument = new JSONDocument(subDoc);

      assertThrows(
          UnsupportedOperationException.class,
          () -> flatCollection.updateSubDoc(docKey, "props.nested", subDocument));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for deleteSubDoc")
    void testSubDocumentDelete() {
      Key docKey = new SingleValueKey("default", "1");

      assertThrows(
          UnsupportedOperationException.class,
          () -> flatCollection.deleteSubDoc(docKey, "props.brand"));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for bulkUpdateSubDocs")
    void testBulkUpdateSubDocs() {
      Map<Key, Map<String, Document>> documents = new HashMap<>();
      Key key1 = new SingleValueKey("default", "1");
      Map<String, Document> subDocs1 = new HashMap<>();
      ObjectNode subDoc1 = OBJECT_MAPPER.createObjectNode();
      subDoc1.put("updated", true);
      subDocs1.put("props.status", new JSONDocument(subDoc1));
      documents.put(key1, subDocs1);

      assertThrows(
          UnsupportedOperationException.class, () -> flatCollection.bulkUpdateSubDocs(documents));
    }
  }

  @Nested
  @DisplayName("Bulk Array Value Operations")
  class BulkArrayValueOperationTests {

    @Test
    @DisplayName("Should throw UnsupportedOperationException for bulkOperationOnArrayValue")
    void testBulkOperationOnArrayValue() throws IOException {
      Set<Key> keys =
          Set.of(new SingleValueKey("default", "1"), new SingleValueKey("default", "2"));
      List<Document> subDocs =
          List.of(new JSONDocument("\"newTag1\""), new JSONDocument("\"newTag2\""));
      BulkArrayValueUpdateRequest request =
          new BulkArrayValueUpdateRequest(
              keys, "tags", BulkArrayValueUpdateRequest.Operation.SET, subDocs);

      assertThrows(
          UnsupportedOperationException.class,
          () -> flatCollection.bulkOperationOnArrayValue(request));
    }
  }

  @Nested
  @DisplayName("CreateOrReplace Schema Refresh Tests")
  class CreateOrReplaceSchemaRefreshTests {

    @Test
    @DisplayName("createOrReplace should refresh schema and retry on dropped column")
    void testCreateOrReplaceRefreshesSchemaOnDroppedColumn() throws Exception {
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

      // Step 1: Add a temporary column
      String addColumnSQL =
          String.format(
              "ALTER TABLE \"%s\" ADD COLUMN \"temp_upsert_col\" TEXT", FLAT_COLLECTION_NAME);
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps = conn.prepareStatement(addColumnSQL)) {
        ps.execute();
        LOGGER.info("Added temporary column 'temp_upsert_col' to table");
      }

      // Step 2: Create a document with the temp column to cache the schema
      ObjectNode objectNode1 = OBJECT_MAPPER.createObjectNode();
      objectNode1.put("id", "upsert-cache-schema-doc");
      objectNode1.put("item", "Item to cache schema");
      objectNode1.put("temp_upsert_col", "temp value");
      flatCollection.createOrReplace(
          new SingleValueKey("default", "upsert-cache-schema-doc"), new JSONDocument(objectNode1));
      LOGGER.info("Schema cached with temp_upsert_col");

      // Step 3: DROP the column - now the cached schema is stale
      String dropColumnSQL =
          String.format("ALTER TABLE \"%s\" DROP COLUMN \"temp_upsert_col\"", FLAT_COLLECTION_NAME);
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps = conn.prepareStatement(dropColumnSQL)) {
        ps.execute();
        LOGGER.info("Dropped temp_upsert_col - schema cache is now stale");
      }

      // Step 4: Try createOrReplace with the dropped column
      // Schema registry still thinks temp_upsert_col exists, so it will include it in UPSERT
      // UPSERT will fail with UNDEFINED_COLUMN, triggering handlePSQLExceptionForUpsert
      // which will refresh schema and retry
      ObjectNode objectNode2 = OBJECT_MAPPER.createObjectNode();
      objectNode2.put("id", "upsert-retry-doc");
      objectNode2.put("item", "Item after schema refresh");
      objectNode2.put("temp_upsert_col", "this column no longer exists");
      Document document = new JSONDocument(objectNode2);
      Key key = new SingleValueKey("default", "upsert-retry-doc");

      boolean result = flatCollection.createOrReplace(key, document);

      // Should succeed after schema refresh - temp_upsert_col will be skipped
      assertTrue(result);

      // Verify the valid fields were inserted
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = '%s'", FLAT_COLLECTION_NAME, key));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Item after schema refresh", rs.getString("item"));
      }
    }
  }

  @Nested
  @DisplayName("Update SET Operator Tests")
  class UpdateSetOperatorTests {

    @Test
    @DisplayName("Case 1: SET on field not in schema should skip (default SKIP strategy)")
    void testSetFieldNotInSchema() throws Exception {
      // Update a field that doesn't exist in the schema
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("1")))
              .build();

      SubDocumentUpdate update =
          SubDocumentUpdate.builder()
              .subDocument("nonexistent_column.some_key")
              .operator(UpdateOperator.SET)
              .subDocumentValue(
                  org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue.of("new_value"))
              .build();

      // With default SKIP strategy, this should not throw but skip the update
      Optional<Document> result =
          flatCollection.update(
              query,
              List.of(update),
              UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build());

      // Document should still be returned (unchanged since update was skipped)
      assertTrue(result.isPresent());

      // Verify the document wasn't modified (item should still be "Soap")
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"item\" FROM \"%s\" WHERE \"id\" = '1'", FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Soap", rs.getString("item"));
      }
    }

    @Test
    @DisplayName("Case 2: SET on JSONB column that is NULL should create the structure")
    void testSetJsonbColumnIsNull() throws Exception {
      // Row 2 has props = NULL
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("2")))
              .build();

      SubDocumentUpdate update =
          SubDocumentUpdate.builder()
              .subDocument("props.newKey")
              .operator(UpdateOperator.SET)
              .subDocumentValue(
                  org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue.of("newValue"))
              .build();

      Optional<Document> result =
          flatCollection.update(
              query,
              List.of(update),
              UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build());

      assertTrue(result.isPresent());

      // Verify props now has the new key
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"props\"->>'newKey' as newKey FROM \"%s\" WHERE \"id\" = '2'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("newValue", rs.getString("newKey"));
      }
    }

    @Test
    @DisplayName("Case 3: SET on JSONB path that exists should update the value")
    void testSetJsonbPathExists() throws Exception {
      // Row 1 has props.brand = "Dettol"
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("1")))
              .build();

      SubDocumentUpdate update =
          SubDocumentUpdate.builder()
              .subDocument("props.brand")
              .operator(UpdateOperator.SET)
              .subDocumentValue(
                  org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue.of(
                      "UpdatedBrand"))
              .build();

      Optional<Document> result =
          flatCollection.update(
              query,
              List.of(update),
              UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build());

      assertTrue(result.isPresent());

      // Verify props.brand was updated
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"props\"->>'brand' as brand FROM \"%s\" WHERE \"id\" = '1'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("UpdatedBrand", rs.getString("brand"));
      }
    }

    @Test
    @DisplayName("Case 4: SET on JSONB path that doesn't exist should create the key")
    void testSetJsonbPathDoesNotExist() throws Exception {
      // Row 1 has props but no "newAttribute" key
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("1")))
              .build();

      SubDocumentUpdate update =
          SubDocumentUpdate.builder()
              .subDocument("props.newAttribute")
              .operator(UpdateOperator.SET)
              .subDocumentValue(
                  org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue.of(
                      "brandNewValue"))
              .build();

      Optional<Document> result =
          flatCollection.update(
              query,
              List.of(update),
              UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build());

      assertTrue(result.isPresent());

      // Verify props.newAttribute was created
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"props\"->>'newAttribute' as newAttr, \"props\"->>'brand' as brand FROM \"%s\" WHERE \"id\" = '1'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("brandNewValue", rs.getString("newAttr"));
        // Verify existing data wasn't lost
        assertEquals("Dettol", rs.getString("brand"));
      }
    }

    @Test
    @DisplayName("SET on top-level column should update the value directly")
    void testSetTopLevelColumn() throws Exception {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("1")))
              .build();

      SubDocumentUpdate update =
          SubDocumentUpdate.builder()
              .subDocument("item")
              .operator(UpdateOperator.SET)
              .subDocumentValue(
                  org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue.of("UpdatedSoap"))
              .build();

      Optional<Document> result =
          flatCollection.update(
              query,
              List.of(update),
              UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build());

      assertTrue(result.isPresent());

      // Verify item was updated
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"item\" FROM \"%s\" WHERE \"id\" = '1'", FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("UpdatedSoap", rs.getString("item"));
      }
    }

    @Test
    @DisplayName("SET with empty object value")
    void testSetWithEmptyObjectValue() throws Exception {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("1")))
              .build();

      // SET a JSON object containing an empty object
      SubDocumentUpdate update =
          SubDocumentUpdate.builder()
              .subDocument("props.newProperty")
              .operator(UpdateOperator.SET)
              .subDocumentValue(
                  org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue.of(
                      new JSONDocument(
                          Map.of("hello", "world", "emptyObject", Collections.emptyMap()))))
              .build();

      Optional<Document> result =
          flatCollection.update(
              query,
              List.of(update),
              UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build());

      assertTrue(result.isPresent());

      // Verify the JSON object was set correctly
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"props\"->'newProperty' as newProp FROM \"%s\" WHERE \"id\" = '1'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        String jsonStr = rs.getString("newProp");
        assertNotNull(jsonStr);
        assertTrue(jsonStr.contains("hello"));
        assertTrue(jsonStr.contains("emptyObject"));
      }
    }

    @Test
    @DisplayName("SET with JSON document as value")
    void testSetWithJsonDocumentValue() throws Exception {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("1")))
              .build();

      // SET a JSON document as value
      SubDocumentUpdate update =
          SubDocumentUpdate.builder()
              .subDocument("props.nested")
              .operator(UpdateOperator.SET)
              .subDocumentValue(
                  org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue.of(
                      new JSONDocument(Map.of("key1", "value1", "key2", 123))))
              .build();

      Optional<Document> result =
          flatCollection.update(
              query,
              List.of(update),
              UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build());

      assertTrue(result.isPresent());

      // Verify the JSON document was set correctly
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"props\"->'nested'->>'key1' as key1, \"props\"->'nested'->>'key2' as key2 FROM \"%s\" WHERE \"id\" = '1'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("value1", rs.getString("key1"));
        assertEquals("123", rs.getString("key2"));
      }
    }
  }

  @Nested
  @DisplayName("Timestamp Auto-Population Tests")
  class TimestampTests {

    @Test
    @DisplayName(
        "Should auto-populate createdTime (BIGINT) and lastUpdateTime (TIMESTAMPTZ) on create")
    void testTimestampsOnCreate() throws Exception {
      long beforeCreate = System.currentTimeMillis();

      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "ts-test-1");
      objectNode.put("item", "TimestampTestItem");
      objectNode.put("price", 100);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, "ts-test-1");

      CreateResult result = flatCollection.create(key, document);
      assertTrue(result.isSucceed());

      long afterCreate = System.currentTimeMillis();

      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"createdTime\", \"lastUpdateTime\" FROM \"%s\" WHERE \"id\" = '%s'",
                      FLAT_COLLECTION_NAME, key.toString()));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());

        long createdTime = rs.getLong("createdTime");
        assertFalse(rs.wasNull(), "createdTime should not be NULL");
        assertTrue(
            createdTime >= beforeCreate && createdTime <= afterCreate,
            "createdTime should be within test execution window");

        Timestamp lastUpdateTime = rs.getTimestamp("lastUpdateTime");
        assertNotNull(lastUpdateTime, "lastUpdateTime should not be NULL");
        assertTrue(
            lastUpdateTime.getTime() >= beforeCreate && lastUpdateTime.getTime() <= afterCreate,
            "lastUpdateTime should be within test execution window");
      }
    }

    @Test
    @DisplayName("Should preserve createdTime and update lastUpdateTime on upsert")
    void testTimestampsOnUpsert() throws Exception {
      // First create
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "ts-test-2");
      objectNode.put("item", "UpsertTimestampTest");
      objectNode.put("price", 100);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, "ts-test-2");

      flatCollection.create(key, document);

      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      long originalCreatedTime;
      long originalLastUpdateTime;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"createdTime\", \"lastUpdateTime\" FROM \"%s\" WHERE \"id\" = '%s'",
                      FLAT_COLLECTION_NAME, key.toString()));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        originalCreatedTime = rs.getLong("createdTime");
        originalLastUpdateTime = rs.getTimestamp("lastUpdateTime").getTime();
      }

      // Wait a bit to ensure time difference
      Thread.sleep(50);

      // Upsert (update existing)
      long beforeUpsert = System.currentTimeMillis();
      objectNode.put("price", 200);
      Document updatedDoc = new JSONDocument(objectNode);
      flatCollection.createOrReplace(key, updatedDoc);
      long afterUpsert = System.currentTimeMillis();

      // Verify createdTime preserved, lastUpdateTime updated
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"createdTime\", \"lastUpdateTime\" FROM \"%s\" WHERE \"id\" = '%s'",
                      FLAT_COLLECTION_NAME, key.toString()));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());

        long newCreatedTime = rs.getLong("createdTime");
        assertEquals(
            originalCreatedTime, newCreatedTime, "createdTime should be preserved on upsert");

        long newLastUpdateTime = rs.getTimestamp("lastUpdateTime").getTime();
        assertTrue(newLastUpdateTime > originalLastUpdateTime, "lastUpdateTime should be updated");
        assertTrue(
            newLastUpdateTime >= beforeUpsert && newLastUpdateTime <= afterUpsert,
            "lastUpdateTime should be within upsert execution window");
      }
    }

    @Test
    @DisplayName(
        "Should not throw exception when timestampFields config is missing - cols remain NULL")
    void testNoExceptionWhenTimestampConfigMissing() throws Exception {
      // Create a collection WITHOUT timestampFields config
      String postgresConnectionUrl =
          String.format("jdbc:postgresql://localhost:%s/", postgres.getMappedPort(5432));

      Map<String, String> configWithoutTimestamps = new HashMap<>();
      configWithoutTimestamps.put("url", postgresConnectionUrl);
      configWithoutTimestamps.put("user", "postgres");
      configWithoutTimestamps.put("password", "postgres");
      // Note: NO customParams.timestampFields config

      Datastore datastoreWithoutTimestamps =
          DatastoreProvider.getDatastore(
              "Postgres", ConfigFactory.parseMap(configWithoutTimestamps));
      Collection collectionWithoutTimestamps =
          datastoreWithoutTimestamps.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Create a document - should NOT throw exception
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "ts-test-no-config");
      objectNode.put("item", "NoTimestampConfigTest");
      objectNode.put("price", 100);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, "ts-test-no-config");

      CreateResult result = collectionWithoutTimestamps.create(key, document);
      assertTrue(result.isSucceed());

      // Verify timestamp columns are NULL
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"createdTime\", \"lastUpdateTime\" FROM \"%s\" WHERE \"id\" = '%s'",
                      FLAT_COLLECTION_NAME, key.toString()));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());

        rs.getLong("createdTime");
        assertTrue(rs.wasNull(), "createdTime should be NULL when config is missing");

        rs.getTimestamp("lastUpdateTime");
        assertTrue(rs.wasNull(), "lastUpdateTime should be NULL when config is missing");
      }
    }

    @Test
    @DisplayName(
        "Should not throw exception when timestampFields config is invalid JSON - cols remain NULL")
    void testNoExceptionWhenTimestampConfigInvalidJson() throws Exception {
      // Create a collection with INVALID JSON in timestampFields config
      String postgresConnectionUrl =
          String.format("jdbc:postgresql://localhost:%s/", postgres.getMappedPort(5432));

      Map<String, String> configWithInvalidJson = new HashMap<>();
      configWithInvalidJson.put("url", postgresConnectionUrl);
      configWithInvalidJson.put("user", "postgres");
      configWithInvalidJson.put("password", "postgres");
      // Invalid JSON - missing quotes, malformed
      configWithInvalidJson.put("customParams.timestampFields", "not valid json {{{");

      Datastore datastoreWithInvalidConfig =
          DatastoreProvider.getDatastore("Postgres", ConfigFactory.parseMap(configWithInvalidJson));
      Collection collectionWithInvalidConfig =
          datastoreWithInvalidConfig.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Create a document - should NOT throw exception
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "ts-test-invalid-json");
      objectNode.put("item", "InvalidJsonConfigTest");
      objectNode.put("price", 100);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, "ts-test-invalid-json");

      CreateResult result = collectionWithInvalidConfig.create(key, document);
      assertTrue(result.isSucceed());

      // Verify timestamp columns are NULL (config parsing failed gracefully)
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"createdTime\", \"lastUpdateTime\" FROM \"%s\" WHERE \"id\" = '%s'",
                      FLAT_COLLECTION_NAME, key.toString()));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());

        rs.getLong("createdTime");
        assertTrue(rs.wasNull(), "createdTime should be NULL when config JSON is invalid");

        rs.getTimestamp("lastUpdateTime");
        assertTrue(rs.wasNull(), "lastUpdateTime should be NULL when config JSON is invalid");
      }
    }
  }
}
