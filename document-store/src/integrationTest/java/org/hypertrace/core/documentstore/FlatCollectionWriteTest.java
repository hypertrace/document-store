package org.hypertrace.core.documentstore;

import static org.hypertrace.core.documentstore.utils.Utils.readFileFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
  @Disabled("Upsert operations not yet implemented for flat collections")
  class UpsertTests {

    @Test
    @DisplayName("Should insert new document when key does not exist")
    void testUpsertNewDocument() throws IOException {
      assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("_id", 100);
      objectNode.put("item", "NewItem");
      objectNode.put("price", 99);
      objectNode.put("quantity", 10);
      objectNode.put("in_stock", true);
      Document document = new JSONDocument(objectNode);

      Key key = new SingleValueKey("default", "100");
      boolean result = flatCollection.upsert(key, document);
      assertTrue(result);

      assertEquals(INITIAL_ROW_COUNT + 1, flatCollection.count());
    }

    @Test
    @DisplayName("Should update existing document when key exists")
    void testUpsertUpdatesExistingDocument() throws IOException {
      assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("_id", 1);
      objectNode.put("item", "Soap Premium");
      objectNode.put("price", 25);
      objectNode.put("quantity", 100);
      Document document = new JSONDocument(objectNode);

      Key key = new SingleValueKey("default", "1");
      flatCollection.upsert(key, document);

      assertEquals(INITIAL_ROW_COUNT, flatCollection.count());
    }

    @Test
    @DisplayName("Should upsert document with JSONB field")
    void testUpsertWithJsonbField() throws IOException {
      assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("_id", 400);
      objectNode.put("item", "ItemWithProps");
      objectNode.put("price", 50);

      ObjectNode propsNode = OBJECT_MAPPER.createObjectNode();
      propsNode.put("color", "green");
      propsNode.put("size", "XL");
      objectNode.set("props", propsNode);

      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "400");

      boolean result = flatCollection.upsert(key, document);
      assertTrue(result);
      assertEquals(INITIAL_ROW_COUNT + 1, flatCollection.count());
    }
  }

  @Nested
  @DisplayName("Create Operations")
  @Disabled("Create operations not yet implemented for flat collections")
  class CreateTests {

    @Test
    @DisplayName("Should create new document")
    void testCreate() throws IOException {
      assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("_id", 300);
      objectNode.put("item", "Brand New Item");
      objectNode.put("price", 15);
      Document document = new JSONDocument(objectNode);

      Key key = new SingleValueKey("default", "300");
      CreateResult result = flatCollection.create(key, document);

      assertNotNull(result);
      assertEquals(INITIAL_ROW_COUNT + 1, flatCollection.count());
    }

    @Test
    @DisplayName("Should create or replace document")
    void testCreateOrReplace() throws IOException {
      assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("_id", 200);
      objectNode.put("item", "NewMirror");
      objectNode.put("price", 30);
      Document document = new JSONDocument(objectNode);

      Key key = new SingleValueKey("default", "200");

      boolean created = flatCollection.createOrReplace(key, document);
      assertTrue(created);
      assertEquals(INITIAL_ROW_COUNT + 1, flatCollection.count());

      ObjectNode updatedNode = OBJECT_MAPPER.createObjectNode();
      updatedNode.put("_id", 200);
      updatedNode.put("item", "Mirror Deluxe");
      updatedNode.put("price", 50);
      Document updatedDocument = new JSONDocument(updatedNode);

      boolean createdAgain = flatCollection.createOrReplace(key, updatedDocument);
      assertFalse(createdAgain);
      assertEquals(INITIAL_ROW_COUNT + 1, flatCollection.count());
    }
  }

  @Nested
  @DisplayName("Bulk Operations")
  @Disabled("Bulk operations not yet implemented for flat collections")
  class BulkOperationTests {

    @Test
    @DisplayName("Should bulk upsert multiple documents")
    void testBulkUpsert() {
      assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

      Map<Key, Document> bulkMap = new HashMap<>();
      for (int i = 101; i <= 105; i++) {
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("_id", i);
        node.put("item", "BulkItem" + i);
        node.put("price", i * 10);
        bulkMap.put(new SingleValueKey("default", String.valueOf(i)), new JSONDocument(node));
      }

      boolean result = flatCollection.bulkUpsert(bulkMap);
      assertTrue(result);

      assertEquals(INITIAL_ROW_COUNT + 5, flatCollection.count());
    }
  }

  @Nested
  @DisplayName("Delete Operations")
  class DeleteTests {

    @Test
    @DisplayName("Should delete document by key")
    void testDeleteByKey() {
      assertEquals(
          INITIAL_ROW_COUNT,
          flatCollection.count(),
          getPreconditionFailureMessage(
              "DB does not have initial row count of: " + INITIAL_ROW_COUNT));

      Key keyToDelete = new SingleValueKey("default", "1");
      boolean deleted = flatCollection.delete(keyToDelete);
      assertTrue(deleted);

      assertEquals(INITIAL_ROW_COUNT - 1, flatCollection.count());
    }

    @Test
    @DisplayName("Should delete documents by filter")
    @Disabled("Delete by filter not yet implemented for flat collections")
    void testDeleteByFilter() {
      assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

      Filter filter = Filter.eq("item", "Soap");
      boolean deleted = flatCollection.delete(filter);
      assertTrue(deleted);

      assertEquals(7, flatCollection.count());
    }

    @Test
    @DisplayName("Should delete all documents")
    void testDeleteAll() {
      assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

      boolean deleted = flatCollection.deleteAll();
      assertTrue(deleted);

      assertEquals(0, flatCollection.count());
    }
  }

  @Nested
  @DisplayName("Update Operations")
  @Disabled("Update operations not yet implemented for flat collections")
  class UpdateTests {

    @Test
    @DisplayName("Should update document with condition")
    void testUpdateWithCondition() throws IOException {
      assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

      Key key = new SingleValueKey("default", "1");

      ObjectNode updatedNode = OBJECT_MAPPER.createObjectNode();
      updatedNode.put("_id", 1);
      updatedNode.put("item", "Soap");
      updatedNode.put("price", 15);
      updatedNode.put("quantity", 100);

      Filter condition = Filter.eq("price", 10);
      UpdateResult result = flatCollection.update(key, new JSONDocument(updatedNode), condition);

      assertNotNull(result);
      assertEquals(INITIAL_ROW_COUNT, flatCollection.count());
    }
  }

  @Nested
  @DisplayName("Count Operations")
  @Disabled("Count operations rely on bulk upsert which is not yet implemented")
  class CountTests {

    @Test
    @DisplayName("Should return correct count after bulk upsert")
    void testCount() {
      assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

      Map<Key, Document> bulkMap = new HashMap<>();
      for (int i = 201; i <= 203; i++) {
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("_id", i);
        node.put("item", "CountItem" + i);
        bulkMap.put(new SingleValueKey("default", String.valueOf(i)), new JSONDocument(node));
      }
      flatCollection.bulkUpsert(bulkMap);

      assertEquals(INITIAL_ROW_COUNT + 3, flatCollection.count());
    }
  }

  @Nested
  @DisplayName("Sub-Document Operations")
  @Disabled("Sub-document operations not yet implemented for flat collections")
  class SubDocumentTests {

    @Test
    @DisplayName("Should update sub-document at path")
    void testSubDocumentUpdate() throws IOException {
      Key docKey = new SingleValueKey("default", "1");

      ObjectNode subDoc = OBJECT_MAPPER.createObjectNode();
      subDoc.put("newField", "newValue");
      Document subDocument = new JSONDocument(subDoc);

      boolean result = flatCollection.updateSubDoc(docKey, "props.nested", subDocument);
      assertTrue(result);
    }

    @Test
    @DisplayName("Should delete sub-document at path")
    void testSubDocumentDelete() {
      Key docKey = new SingleValueKey("default", "1");

      boolean result = flatCollection.deleteSubDoc(docKey, "props.brand");
      assertTrue(result);
    }

    @Test
    @DisplayName("Should bulk update sub-documents")
    void testBulkUpdateSubDocs() throws Exception {
      Map<Key, Map<String, Document>> documents = new HashMap<>();

      Key key1 = new SingleValueKey("default", "1");
      Map<String, Document> subDocs1 = new HashMap<>();
      ObjectNode subDoc1 = OBJECT_MAPPER.createObjectNode();
      subDoc1.put("updated", true);
      subDocs1.put("props.status", new JSONDocument(subDoc1));
      documents.put(key1, subDocs1);

      BulkUpdateResult result = flatCollection.bulkUpdateSubDocs(documents);
      assertNotNull(result);
    }
  }

  @Nested
  @DisplayName("Bulk Array Value Operations")
  @Disabled("Bulk array value operations not yet implemented for flat collections")
  class BulkArrayValueOperationTests {

    @Test
    @DisplayName("Should set array values in bulk")
    void testBulkOperationOnArrayValue_setOperation() throws Exception {
      Set<Key> keys =
          Set.of(new SingleValueKey("default", "1"), new SingleValueKey("default", "2"));

      List<Document> subDocs =
          List.of(new JSONDocument("\"newTag1\""), new JSONDocument("\"newTag2\""));

      BulkArrayValueUpdateRequest request =
          new BulkArrayValueUpdateRequest(
              keys, "tags", BulkArrayValueUpdateRequest.Operation.SET, subDocs);

      BulkUpdateResult result = flatCollection.bulkOperationOnArrayValue(request);
      assertNotNull(result);
    }

    @Test
    @DisplayName("Should add array values in bulk")
    void testBulkOperationOnArrayValue_addOperation() throws Exception {
      Set<Key> keys =
          Set.of(new SingleValueKey("default", "1"), new SingleValueKey("default", "2"));

      List<Document> subDocs = List.of(new JSONDocument("\"addedTag\""));

      BulkArrayValueUpdateRequest request =
          new BulkArrayValueUpdateRequest(
              keys, "tags", BulkArrayValueUpdateRequest.Operation.ADD, subDocs);

      BulkUpdateResult result = flatCollection.bulkOperationOnArrayValue(request);
      assertNotNull(result);
    }

    @Test
    @DisplayName("Should remove array values in bulk")
    void testBulkOperationOnArrayValue_removeOperation() throws Exception {
      Set<Key> keys =
          Set.of(new SingleValueKey("default", "1"), new SingleValueKey("default", "2"));

      List<Document> subDocs = List.of(new JSONDocument("\"hygiene\""));

      BulkArrayValueUpdateRequest request =
          new BulkArrayValueUpdateRequest(
              keys, "tags", BulkArrayValueUpdateRequest.Operation.REMOVE, subDocs);

      BulkUpdateResult result = flatCollection.bulkOperationOnArrayValue(request);
      assertNotNull(result);
    }
  }

  // ==================== Helper Methods ====================

  private static String getPreconditionFailureMessage(String message) {
    return String.format("%s: %s", "Precondition failure:", message);
  }
}
