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
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.documentstore.model.exception.DuplicateDocumentException;
import org.hypertrace.core.documentstore.model.exception.SchemaMismatchException;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
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
  // Initial data has 10 rows (IDs 1-10)
  private static final int INITIAL_ROW_COUNT = 10;

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
                + "\"weight\" DOUBLE PRECISION"
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
    @DisplayName("Should create a new document with all field types")
    void testCreateNewDocument() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "new-doc-100");
      objectNode.put("item", "Brand New Item");
      objectNode.put("price", 999);
      objectNode.put("quantity", 50);
      objectNode.put("in_stock", true);
      objectNode.set("tags", OBJECT_MAPPER.createArrayNode().add("electronics").add("sale"));

      // Add JSONB field
      ObjectNode propsNode = OBJECT_MAPPER.createObjectNode();
      propsNode.put("color", "blue");
      propsNode.put("weight", 2.5);
      propsNode.put("warranty", true);
      objectNode.set("props", propsNode);

      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "new-doc-100");

      CreateResult result = flatCollection.create(key, document);

      assertTrue(result.isSucceed());

      // Verify the data was inserted
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = 'new-doc-100'", FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Brand New Item", rs.getString("item"));
        assertEquals(999, rs.getInt("price"));
        assertEquals(50, rs.getInt("quantity"));
        assertTrue(rs.getBoolean("in_stock"));

        // Verify tags array
        java.sql.Array tagsArray = rs.getArray("tags");
        assertNotNull(tagsArray);
        String[] tags = (String[]) tagsArray.getArray();
        assertEquals(2, tags.length);
        assertEquals("electronics", tags[0]);
        assertEquals("sale", tags[1]);

        // Verify JSONB props
        String propsJson = rs.getString("props");
        assertNotNull(propsJson);
        JsonNode propsResult = OBJECT_MAPPER.readTree(propsJson);
        assertEquals("blue", propsResult.get("color").asText());
        assertEquals(2.5, propsResult.get("weight").asDouble(), 0.01);
        assertTrue(propsResult.get("warranty").asBoolean());
      }
    }

    @Test
    @DisplayName("Should throw DuplicateDocumentException when creating with existing key")
    void testCreateDuplicateDocument() throws Exception {
      // First create succeeds
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "dup-doc-200");
      objectNode.put("item", "First Item");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "dup-doc-200");

      flatCollection.create(key, document);

      // Second create with same key should fail
      ObjectNode objectNode2 = OBJECT_MAPPER.createObjectNode();
      objectNode2.put("id", "dup-doc-200");
      objectNode2.put("item", "Second Item");
      Document document2 = new JSONDocument(objectNode2);

      assertThrows(DuplicateDocumentException.class, () -> flatCollection.create(key, document2));
    }

    @Test
    @DisplayName("Should create document with JSONB field")
    void testCreateWithJsonbField() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "jsonb-doc-300");
      objectNode.put("item", "Item with Props");
      ObjectNode propsNode = OBJECT_MAPPER.createObjectNode();
      propsNode.put("color", "blue");
      propsNode.put("size", "large");
      objectNode.set("props", propsNode);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "jsonb-doc-300");

      CreateResult result = flatCollection.create(key, document);

      assertTrue(result.isSucceed());

      // Verify JSONB data
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT props->>'color' as color FROM \"%s\" WHERE \"id\" = 'jsonb-doc-300'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("blue", rs.getString("color"));
      }
    }

    @Test
    @DisplayName("Should skip unknown fields and insert known fields")
    void testCreateSkipsUnknownFields() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "unknown-field-doc-400");
      objectNode.put("item", "Item");
      objectNode.put("unknown_column", "should be skipped");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "unknown-field-doc-400");

      CreateResult result = flatCollection.create(key, document);

      assertTrue(result.isSucceed());

      // Verify only known columns were inserted
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = 'unknown-field-doc-400'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Item", rs.getString("item"));
      }
    }

    @Test
    @DisplayName("Should return skipped fields in CreateResult when columns are missing")
    void testCreateReturnsSkippedFieldsInResult() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "skipped-fields-doc-500");
      objectNode.put("item", "Valid Item");
      objectNode.put("price", 100);
      objectNode.put("nonexistent_field1", "value1");
      objectNode.put("nonexistent_field2", "value2");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "skipped-fields-doc-500");

      CreateResult result = flatCollection.create(key, document);

      assertTrue(result.isSucceed());
      assertTrue(result.isPartial());
      assertNotNull(result.getSkippedFields());
      assertEquals(2, result.getSkippedFields().size());
      assertTrue(
          result
              .getSkippedFields()
              .containsAll(List.of("nonexistent_field1", "nonexistent_field2")));

      // Verify the valid fields were inserted
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = 'skipped-fields-doc-500'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Valid Item", rs.getString("item"));
        assertEquals(100, rs.getInt("price"));
      }
    }

    @Test
    @DisplayName("Should return empty skipped fields when all columns exist")
    void testCreateReturnsEmptySkippedFieldsWhenAllColumnsExist() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "all-valid-doc-600");
      objectNode.put("item", "Valid Item");
      objectNode.put("price", 200);
      objectNode.put("quantity", 10);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "all-valid-doc-600");

      CreateResult result = flatCollection.create(key, document);

      assertTrue(result.isSucceed());
      assertFalse(result.isPartial());
      assertTrue(result.getSkippedFields().isEmpty());
    }

    @Test
    @DisplayName("Should return failure when all fields are unknown (parsed.isEmpty)")
    void testCreateFailsWhenAllFieldsAreUnknown() throws Exception {
      // Document with only unknown fields - no valid columns will be found
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("completely_unknown_field1", "value1");
      objectNode.put("completely_unknown_field2", "value2");
      objectNode.put("another_nonexistent_column", 123);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "all-unknown-doc-700");

      CreateResult result = flatCollection.create(key, document);

      // Should fail because no valid columns found (parsed.isEmpty() == true)
      assertFalse(result.isSucceed());
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
                      "SELECT COUNT(*) FROM \"%s\" WHERE \"id\" = 'all-unknown-doc-700'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
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
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = 'retry-doc-800'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Item after schema refresh", rs.getString("item"));
      }
    }

    @Test
    @DisplayName("Should skip column with unparseable value and add to skippedFields")
    void testCreateSkipsUnparseableValues() throws Exception {
      // Try to insert a string value into an integer column with wrong type
      // The unparseable column should be skipped, not throw an exception
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "datatype-mismatch-doc-900");
      objectNode.put("item", "Valid Item");
      objectNode.put("price", "not_a_number_at_all"); // price is INTEGER, this will fail parsing
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "datatype-mismatch-doc-900");

      CreateResult result = flatCollection.create(key, document);

      // Should succeed with the valid columns, skipping the unparseable one
      assertTrue(result.isSucceed());
      assertTrue(result.isPartial());
      assertEquals(1, result.getSkippedFields().size());
      assertTrue(result.getSkippedFields().contains("price"));

      // Verify the valid fields were inserted
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = 'datatype-mismatch-doc-900'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Valid Item", rs.getString("item"));
        // price should be null since it was skipped
        assertEquals(0, rs.getInt("price"));
        assertTrue(rs.wasNull());
      }
    }

    @Test
    @DisplayName("Should handle all scalar data types including string parsing and nulls")
    void testCreateWithAllDataTypes() throws Exception {
      // Test 1: All types with native values (number nodes, boolean nodes, etc.)
      ObjectNode nativeTypesNode = OBJECT_MAPPER.createObjectNode();
      nativeTypesNode.put("id", "native-types-doc");
      nativeTypesNode.put("item", "Native Types"); // TEXT
      nativeTypesNode.put("price", 100); // INTEGER (number node)
      nativeTypesNode.put("big_number", 9223372036854775807L); // BIGINT (number node)
      nativeTypesNode.put("rating", 4.5f); // REAL (number node)
      nativeTypesNode.put("weight", 123.456789); // DOUBLE PRECISION (number node)
      nativeTypesNode.put("in_stock", true); // BOOLEAN (boolean node)
      nativeTypesNode.put("date", "2024-01-15T10:30:00Z"); // TIMESTAMPTZ (textual)
      nativeTypesNode.put("created_date", "2024-01-15"); // DATE (textual)
      nativeTypesNode.putObject("props").put("key", "value"); // JSONB

      CreateResult result1 =
          flatCollection.create(
              new SingleValueKey("default", "native-types-doc"), new JSONDocument(nativeTypesNode));
      assertTrue(result1.isSucceed());

      // Test 2: String representations of numbers (covers parseInt, parseLong, etc.)
      ObjectNode stringTypesNode = OBJECT_MAPPER.createObjectNode();
      stringTypesNode.put("id", "string-types-doc");
      stringTypesNode.put("item", "String Types");
      stringTypesNode.put("price", "200"); // INTEGER from string
      stringTypesNode.put("big_number", "1234567890123"); // BIGINT from string
      stringTypesNode.put("rating", "3.75"); // REAL from string
      stringTypesNode.put("weight", "987.654"); // DOUBLE PRECISION from string
      stringTypesNode.put("in_stock", "true"); // BOOLEAN from string

      CreateResult result2 =
          flatCollection.create(
              new SingleValueKey("default", "string-types-doc"), new JSONDocument(stringTypesNode));
      assertTrue(result2.isSucceed());

      // Test 3: TIMESTAMPTZ from epoch milliseconds
      long epochMillis = 1705315800000L;
      ObjectNode epochNode = OBJECT_MAPPER.createObjectNode();
      epochNode.put("id", "epoch-doc");
      epochNode.put("item", "Epoch Timestamp");
      epochNode.put("date", epochMillis); // TIMESTAMPTZ from number

      CreateResult result3 =
          flatCollection.create(
              new SingleValueKey("default", "epoch-doc"), new JSONDocument(epochNode));
      assertTrue(result3.isSucceed());

      // Test 4: Null values (covers setParameter null handling)
      ObjectNode nullNode = OBJECT_MAPPER.createObjectNode();
      nullNode.put("id", "null-doc");
      nullNode.put("item", "Null Values");
      nullNode.putNull("price");
      nullNode.putNull("date");
      nullNode.putNull("in_stock");

      CreateResult result4 =
          flatCollection.create(
              new SingleValueKey("default", "null-doc"), new JSONDocument(nullNode));
      assertTrue(result4.isSucceed());

      // Verify all inserts
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" IN ('native-types-doc', 'string-types-doc', 'epoch-doc', 'null-doc') ORDER BY \"id\"",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {

        // epoch-doc
        assertTrue(rs.next());
        assertEquals(epochMillis, rs.getTimestamp("date").getTime());

        // native-types-doc
        assertTrue(rs.next());
        assertEquals(100, rs.getInt("price"));
        assertEquals(9223372036854775807L, rs.getLong("big_number"));
        assertEquals(4.5f, rs.getFloat("rating"), 0.01f);
        assertEquals(123.456789, rs.getDouble("weight"), 0.0001);
        assertTrue(rs.getBoolean("in_stock"));

        // null-doc
        assertTrue(rs.next());
        rs.getInt("price");
        assertTrue(rs.wasNull());

        // string-types-doc
        assertTrue(rs.next());
        assertEquals(200, rs.getInt("price"));
        assertEquals(1234567890123L, rs.getLong("big_number"));
        assertEquals(3.75f, rs.getFloat("rating"), 0.01f);
      }
    }

    @Test
    @DisplayName("Should handle array types and single-to-array conversion")
    void testCreateWithArrayTypes() throws Exception {
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

      // Test 1: Proper arrays
      ObjectNode arrayNode = OBJECT_MAPPER.createObjectNode();
      arrayNode.put("id", "array-doc");
      arrayNode.put("item", "Array Types");
      arrayNode.putArray("tags").add("tag1").add("tag2"); // TEXT[]
      arrayNode.putArray("numbers").add(10).add(20); // INTEGER[]
      arrayNode.putArray("scores").add(1.5).add(2.5); // DOUBLE PRECISION[]
      arrayNode.putArray("flags").add(true).add(false); // BOOLEAN[]

      CreateResult result1 =
          flatCollection.create(
              new SingleValueKey("default", "array-doc"), new JSONDocument(arrayNode));
      assertTrue(result1.isSucceed());

      // Test 2: Single values auto-converted to arrays
      ObjectNode singleNode = OBJECT_MAPPER.createObjectNode();
      singleNode.put("id", "single-to-array-doc");
      singleNode.put("item", "Single to Array");
      singleNode.put("tags", "single-tag"); // TEXT[] from single value
      singleNode.put("numbers", 42); // INTEGER[] from single value

      CreateResult result2 =
          flatCollection.create(
              new SingleValueKey("default", "single-to-array-doc"), new JSONDocument(singleNode));
      assertTrue(result2.isSucceed());

      // Verify
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" IN ('array-doc', 'single-to-array-doc') ORDER BY \"id\"",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {

        // array-doc
        assertTrue(rs.next());
        assertEquals(2, ((String[]) rs.getArray("tags").getArray()).length);
        assertEquals(2, ((Integer[]) rs.getArray("numbers").getArray()).length);

        // single-to-array-doc
        assertTrue(rs.next());
        String[] tags = (String[]) rs.getArray("tags").getArray();
        assertEquals(1, tags.length);
        assertEquals("single-tag", tags[0]);
      }
    }
  }

  @Nested
  @DisplayName("CreateOrReplace Operations")
  class CreateOrReplaceTests {

    @Test
    @DisplayName("Should create new document and return true")
    void testCreateOrReplaceNewDocument() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "upsert-new-doc-100");
      objectNode.put("item", "New Upsert Item");
      objectNode.put("price", 500);
      objectNode.put("quantity", 25);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "upsert-new-doc-100");

      boolean isNew = flatCollection.createOrReplace(key, document);

      assertTrue(isNew); // Should return true for new document

      // Verify the data was inserted
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = 'upsert-new-doc-100'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("New Upsert Item", rs.getString("item"));
        assertEquals(500, rs.getInt("price"));
        assertEquals(25, rs.getInt("quantity"));
      }
    }

    @Test
    @DisplayName("Should replace existing document and return false")
    void testCreateOrReplaceExistingDocument() throws Exception {
      // First create a document
      ObjectNode initialNode = OBJECT_MAPPER.createObjectNode();
      initialNode.put("id", "upsert-existing-doc-200");
      initialNode.put("item", "Original Item");
      initialNode.put("price", 100);
      Document initialDoc = new JSONDocument(initialNode);
      Key key = new SingleValueKey("default", "upsert-existing-doc-200");

      boolean firstResult = flatCollection.createOrReplace(key, initialDoc);
      assertTrue(firstResult); // First insert should return true

      // Now replace with updated document
      ObjectNode updatedNode = OBJECT_MAPPER.createObjectNode();
      updatedNode.put("id", "upsert-existing-doc-200");
      updatedNode.put("item", "Updated Item");
      updatedNode.put("price", 999);
      updatedNode.put("quantity", 50);
      Document updatedDoc = new JSONDocument(updatedNode);

      boolean secondResult = flatCollection.createOrReplace(key, updatedDoc);
      assertFalse(secondResult); // Update should return false

      // Verify the data was updated
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = 'upsert-existing-doc-200'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Updated Item", rs.getString("item"));
        assertEquals(999, rs.getInt("price"));
        assertEquals(50, rs.getInt("quantity"));
      }
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
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = 'upsert-skip-fields-300'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Item with unknown", rs.getString("item"));
        assertEquals(200, rs.getInt("price"));
      }
    }

    @Test
    @DisplayName("Should handle JSONB fields in createOrReplace")
    void testCreateOrReplaceWithJsonbField() throws Exception {
      // Create initial document with JSONB
      ObjectNode initialNode = OBJECT_MAPPER.createObjectNode();
      initialNode.put("id", "upsert-jsonb-400");
      initialNode.put("item", "Item with props");
      ObjectNode initialProps = OBJECT_MAPPER.createObjectNode();
      initialProps.put("color", "red");
      initialProps.put("size", "small");
      initialNode.set("props", initialProps);
      Document initialDoc = new JSONDocument(initialNode);
      Key key = new SingleValueKey("default", "upsert-jsonb-400");

      flatCollection.createOrReplace(key, initialDoc);

      // Update with new JSONB value
      ObjectNode updatedNode = OBJECT_MAPPER.createObjectNode();
      updatedNode.put("id", "upsert-jsonb-400");
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
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"props\" FROM \"%s\" WHERE \"id\" = 'upsert-jsonb-400'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        JsonNode propsResult = OBJECT_MAPPER.readTree(rs.getString("props"));
        assertEquals("blue", propsResult.get("color").asText());
        assertEquals("large", propsResult.get("size").asText());
        assertEquals(2.5, propsResult.get("weight").asDouble(), 0.01);
      }
    }

    @Test
    @DisplayName("Should throw IOException when id column is missing")
    void testCreateOrReplaceWithoutIdColumn() {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("item", "No ID Item");
      objectNode.put("price", 100);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "no-id-key");

      IOException exception =
          assertThrows(IOException.class, () -> flatCollection.createOrReplace(key, document));
      assertTrue(exception.getMessage().contains("primary key"));
    }

    @Test
    @DisplayName("Should return false when all fields are unknown (no valid columns)")
    void testCreateOrReplaceAllFieldsUnknown() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("unknown1", "value1");
      objectNode.put("unknown2", "value2");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "all-unknown-key");

      boolean result = flatCollection.createOrReplace(key, document);
      assertFalse(result); // Should return false when no valid columns
    }
  }

  @Nested
  @DisplayName("Bulk Operations")
  class BulkOperationTests {

    @Test
    @DisplayName("Should throw UnsupportedOperationException for bulkUpsert")
    void testBulkUpsert() {
      Map<Key, Document> bulkMap = new HashMap<>();
      ObjectNode node = OBJECT_MAPPER.createObjectNode();
      node.put("_id", 101);
      node.put("item", "BulkItem101");
      bulkMap.put(new SingleValueKey("default", "101"), new JSONDocument(node));

      assertThrows(UnsupportedOperationException.class, () -> flatCollection.bulkUpsert(bulkMap));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for bulkUpsertAndReturnOlderDocuments")
    void testBulkUpsertAndReturnOlderDocuments() {
      Map<Key, Document> bulkMap = new HashMap<>();
      ObjectNode node = OBJECT_MAPPER.createObjectNode();
      node.put("_id", 101);
      bulkMap.put(new SingleValueKey("default", "101"), new JSONDocument(node));

      assertThrows(
          UnsupportedOperationException.class,
          () -> flatCollection.bulkUpsertAndReturnOlderDocuments(bulkMap));
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
  @DisplayName("Strict Mode (missingColumnStrategy=THROW)")
  class StrictModeTests {

    private Datastore strictDatastore;
    private Collection strictCollection;

    @BeforeEach
    void setupStrictModeDatastore() {
      // Create a datastore with missingColumnStrategy=THROW (strict mode)
      String postgresConnectionUrl =
          String.format("jdbc:postgresql://localhost:%s/", postgres.getMappedPort(5432));

      Map<String, Object> strictConfig = new HashMap<>();
      strictConfig.put("url", postgresConnectionUrl);
      strictConfig.put("user", "postgres");
      strictConfig.put("password", "postgres");
      // Configure strict mode via customParams
      Map<String, String> customParams = new HashMap<>();
      customParams.put("missingColumnStrategy", "THROW");
      strictConfig.put("customParams", customParams);

      strictDatastore =
          DatastoreProvider.getDatastore("Postgres", ConfigFactory.parseMap(strictConfig));
      strictCollection =
          strictDatastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);
    }

    @Test
    @DisplayName("Should throw SchemaMismatchException when column not in schema (strict mode)")
    void testStrictModeThrowsOnUnknownColumn() {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "strict-unknown-col-doc");
      objectNode.put("item", "Valid Item");
      objectNode.put("unknown_column", "this should fail");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "strict-unknown-col-doc");

      SchemaMismatchException exception =
          assertThrows(SchemaMismatchException.class, () -> strictCollection.create(key, document));

      assertTrue(exception.getMessage().contains("unknown_column"));
      assertTrue(exception.getMessage().contains("not found in schema"));
    }

    @Test
    @DisplayName(
        "Should throw SchemaMismatchException when value type doesn't match schema (strict mode)")
    void testStrictModeThrowsOnTypeMismatch() {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "strict-type-mismatch-doc");
      objectNode.put("item", "Valid Item");
      objectNode.put("price", "not_a_number_at_all"); // price is INTEGER
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "strict-type-mismatch-doc");

      SchemaMismatchException exception =
          assertThrows(SchemaMismatchException.class, () -> strictCollection.create(key, document));

      assertTrue(exception.getMessage().contains("price"));
      assertTrue(exception.getMessage().contains("Failed to parse value"));
    }

    @Test
    @DisplayName("Should succeed in strict mode when all fields match schema")
    void testStrictModeSucceedsWithValidDocument() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "strict-valid-doc");
      objectNode.put("item", "Valid Item");
      objectNode.put("price", 100);
      objectNode.put("quantity", 5);
      objectNode.put("in_stock", true);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "strict-valid-doc");

      CreateResult result = strictCollection.create(key, document);

      assertTrue(result.isSucceed());
      assertFalse(result.isPartial());
      assertTrue(result.getSkippedFields().isEmpty());

      // Verify data was inserted
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = 'strict-valid-doc'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Valid Item", rs.getString("item"));
        assertEquals(100, rs.getInt("price"));
      }
    }

    @Test
    @DisplayName("Should throw SchemaMismatchException on first unknown field (strict mode)")
    void testStrictModeFailsFastOnFirstUnknownField() {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "strict-multi-unknown-doc");
      objectNode.put("unknown_field_1", "value1");
      objectNode.put("unknown_field_2", "value2");
      objectNode.put("item", "Valid Item");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "strict-multi-unknown-doc");

      // Should throw on the first unknown field encountered
      assertThrows(SchemaMismatchException.class, () -> strictCollection.create(key, document));
    }
  }

  @Nested
  @DisplayName("Ignore Document Mode (missingColumnStrategy=IGNORE_DOCUMENT)")
  class IgnoreDocumentModeTests {

    private Datastore ignoreDocDatastore;
    private Collection ignoreDocCollection;

    @BeforeEach
    void setupIgnoreDocumentModeDatastore() {
      // Create a datastore with missingColumnStrategy=IGNORE_DOCUMENT
      String postgresConnectionUrl =
          String.format("jdbc:postgresql://localhost:%s/", postgres.getMappedPort(5432));

      Map<String, Object> config = new HashMap<>();
      config.put("url", postgresConnectionUrl);
      config.put("user", "postgres");
      config.put("password", "postgres");
      Map<String, String> customParams = new HashMap<>();
      customParams.put("missingColumnStrategy", "IGNORE_DOCUMENT");
      config.put("customParams", customParams);

      ignoreDocDatastore =
          DatastoreProvider.getDatastore("Postgres", ConfigFactory.parseMap(config));
      ignoreDocCollection =
          ignoreDocDatastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);
    }

    @Test
    @DisplayName("Should return IGNORED status when document has unknown columns")
    void testIgnoreDocumentWithUnknownColumn() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "ignore-doc-unknown-col");
      objectNode.put("item", "Valid Item");
      objectNode.put("unknown_column", "this should cause ignore");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "ignore-doc-unknown-col");

      CreateResult result = ignoreDocCollection.create(key, document);

      assertFalse(result.isSucceed());
      assertTrue(result.isDocumentIgnored());
      assertTrue(result.getSkippedFields().contains("unknown_column"));

      // Verify no row was inserted
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT COUNT(*) FROM \"%s\" WHERE \"id\" = 'ignore-doc-unknown-col'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }

    @Test
    @DisplayName("Should return IGNORED status when document has type mismatch")
    void testIgnoreDocumentWithTypeMismatch() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "ignore-doc-type-mismatch");
      objectNode.put("item", "Valid Item");
      objectNode.put("price", "not_a_number"); // price is INTEGER
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "ignore-doc-type-mismatch");

      CreateResult result = ignoreDocCollection.create(key, document);

      assertFalse(result.isSucceed());
      assertTrue(result.isDocumentIgnored());
      assertTrue(result.getSkippedFields().contains("price"));

      // Verify no row was inserted
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT COUNT(*) FROM \"%s\" WHERE \"id\" = 'ignore-doc-type-mismatch'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }

    @Test
    @DisplayName("Should succeed when all fields match schema (IGNORE_DOCUMENT mode)")
    void testIgnoreDocumentSucceedsWithValidDocument() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "ignore-doc-valid");
      objectNode.put("item", "Valid Item");
      objectNode.put("price", 100);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "ignore-doc-valid");

      CreateResult result = ignoreDocCollection.create(key, document);

      assertTrue(result.isSucceed());
      assertFalse(result.isDocumentIgnored());
      assertTrue(result.getSkippedFields().isEmpty());

      // Verify data was inserted
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = 'ignore-doc-valid'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Valid Item", rs.getString("item"));
        assertEquals(100, rs.getInt("price"));
      }
    }

    @Test
    @DisplayName("Should return all problematic fields in skippedFields when ignored")
    void testIgnoreDocumentReturnsAllSkippedFields() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "ignore-doc-multi-issues");
      objectNode.put("item", "Valid Item");
      objectNode.put("unknown_field_1", "value1");
      objectNode.put("unknown_field_2", "value2");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "ignore-doc-multi-issues");

      CreateResult result = ignoreDocCollection.create(key, document);

      assertFalse(result.isSucceed());
      assertTrue(result.isDocumentIgnored());
      assertEquals(2, result.getSkippedFields().size());
      assertTrue(
          result.getSkippedFields().containsAll(List.of("unknown_field_1", "unknown_field_2")));
    }
  }
}
