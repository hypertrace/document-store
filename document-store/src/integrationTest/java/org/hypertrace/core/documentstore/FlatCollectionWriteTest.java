package org.hypertrace.core.documentstore;

import static org.hypertrace.core.documentstore.utils.Utils.readFileFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
                + "\"flags\" BOOLEAN[]"
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
    @DisplayName("Should create a new document when key does not exist")
    void testCreateOrReplaceNewDocument() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "createorreplace-new-500");
      objectNode.put("item", "NewMirror");
      objectNode.put("price", 150);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "createorreplace-new-500");

      boolean result = flatCollection.createOrReplace(key, document);

      assertTrue(result);

      // Verify the data was inserted
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = 'createorreplace-new-500'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("NewMirror", rs.getString("item"));
        assertEquals(150, rs.getInt("price"));
      }
    }

    @Test
    @DisplayName("Should replace an existing document when key exists")
    void testCreateOrReplaceExistingDocument() throws Exception {
      // First create a document
      ObjectNode objectNode1 = OBJECT_MAPPER.createObjectNode();
      objectNode1.put("id", "createorreplace-existing-600");
      objectNode1.put("item", "OriginalItem");
      objectNode1.put("price", 100);
      Document document1 = new JSONDocument(objectNode1);
      Key key = new SingleValueKey("default", "createorreplace-existing-600");
      flatCollection.create(key, document1);

      // Now replace it
      ObjectNode objectNode2 = OBJECT_MAPPER.createObjectNode();
      objectNode2.put("id", "createorreplace-existing-600");
      objectNode2.put("item", "ReplacedItem");
      objectNode2.put("price", 200);
      Document document2 = new JSONDocument(objectNode2);

      boolean result = flatCollection.createOrReplace(key, document2);

      assertTrue(result);

      // Verify the data was replaced
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = 'createorreplace-existing-600'",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("ReplacedItem", rs.getString("item"));
        assertEquals(200, rs.getInt("price"));
      }
    }

    @Test
    @DisplayName("Should create and return a new document when key does not exist")
    void testCreateOrReplaceAndReturnNewDocument() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "createorreplace-return-700");
      objectNode.put("item", "ReturnedItem");
      objectNode.put("price", 250);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "createorreplace-return-700");

      Document result = flatCollection.createOrReplaceAndReturn(key, document);

      assertNotNull(result);
      JsonNode resultNode = OBJECT_MAPPER.readTree(result.toJson());
      assertEquals("createorreplace-return-700", resultNode.get("id").asText());
      assertEquals("ReturnedItem", resultNode.get("item").asText());
      assertEquals(250, resultNode.get("price").asInt());
    }

    @Test
    @DisplayName("Should replace and return an existing document when key exists")
    void testCreateOrReplaceAndReturnExistingDocument() throws Exception {
      // First create a document
      ObjectNode objectNode1 = OBJECT_MAPPER.createObjectNode();
      objectNode1.put("id", "createorreplace-return-existing-800");
      objectNode1.put("item", "OriginalReturnItem");
      objectNode1.put("price", 300);
      Document document1 = new JSONDocument(objectNode1);
      Key key = new SingleValueKey("default", "createorreplace-return-existing-800");
      flatCollection.create(key, document1);

      // Now replace it
      ObjectNode objectNode2 = OBJECT_MAPPER.createObjectNode();
      objectNode2.put("id", "createorreplace-return-existing-800");
      objectNode2.put("item", "ReplacedReturnItem");
      objectNode2.put("price", 400);
      Document document2 = new JSONDocument(objectNode2);

      Document result = flatCollection.createOrReplaceAndReturn(key, document2);

      assertNotNull(result);
      JsonNode resultNode = OBJECT_MAPPER.readTree(result.toJson());
      assertEquals("createorreplace-return-existing-800", resultNode.get("id").asText());
      assertEquals("ReplacedReturnItem", resultNode.get("item").asText());
      assertEquals(400, resultNode.get("price").asInt());
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
}
