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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.hypertrace.core.documentstore.query.Query;
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
    @DisplayName("Should upsert a new document with TypedDocument")
    void testUpsertNewDocument() throws IOException {
      // Create TypedDocument with value and type coupled together
      TypedDocument typedDoc =
          TypedDocument.builder()
              .field("item", "NewItem", DataType.STRING)
              .field("price", 99, DataType.INTEGER)
              .field("quantity", 50, DataType.INTEGER)
              .field("in_stock", true, DataType.BOOLEAN)
              .arrayField("tags", List.of("new", "test"), DataType.STRING)
              .jsonbField("props", Map.of("brand", "TestBrand", "size", "medium"))
              .build();

      Key key = new SingleValueKey("default", "100");

      // Perform upsert
      boolean result = flatCollection.upsert(key, typedDoc);
      assertTrue(result, "Upsert should return true for new document");

      // Verify the document was inserted by querying it
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"),
                      RelationalOperator.EQ,
                      org.hypertrace.core.documentstore.expression.impl.ConstantExpression.of(
                          "NewItem")))
              .build();

      Iterator<Document> results = flatCollection.find(query);
      assertTrue(results.hasNext(), "Should find the inserted document");

      Document foundDoc = results.next();
      assertNotNull(foundDoc);

      JsonNode foundJson = OBJECT_MAPPER.readTree(foundDoc.toJson());
      assertEquals("NewItem", foundJson.get("item").asText());
      assertEquals(99, foundJson.get("price").asInt());
      assertEquals(50, foundJson.get("quantity").asInt());
      assertTrue(foundJson.get("in_stock").asBoolean());

      // Verify array field
      JsonNode tagsNode = foundJson.get("tags");
      assertNotNull(tagsNode);
      assertTrue(tagsNode.isArray());
      assertEquals(2, tagsNode.size());

      // Verify JSONB field
      JsonNode propsResult = foundJson.get("props");
      assertNotNull(propsResult);
      assertEquals("TestBrand", propsResult.get("brand").asText());
    }

    @Test
    @DisplayName("Should update existing document on upsert with TypedDocument")
    void testUpsertExistingDocument() throws IOException {
      // First, get an existing document ID from the initial data
      String existingId = "1"; // ID 1 exists in initial data

      // Create TypedDocument with updated values
      TypedDocument typedDoc =
          TypedDocument.builder()
              .field("item", "UpdatedSoap", DataType.STRING)
              .field("price", 999, DataType.INTEGER)
              .field("quantity", 100, DataType.INTEGER)
              .field("in_stock", false, DataType.BOOLEAN)
              .build();

      Key key = new SingleValueKey("default", existingId);

      // Perform upsert (should update existing)
      boolean result = flatCollection.upsert(key, typedDoc);
      assertTrue(result, "Upsert should return true for existing document update");

      // Verify the document was updated
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"),
                      RelationalOperator.EQ,
                      org.hypertrace.core.documentstore.expression.impl.ConstantExpression.of(
                          "UpdatedSoap")))
              .build();

      Iterator<Document> results = flatCollection.find(query);
      assertTrue(results.hasNext(), "Should find the updated document");

      Document foundDoc = results.next();
      JsonNode foundJson = OBJECT_MAPPER.readTree(foundDoc.toJson());
      assertEquals("UpdatedSoap", foundJson.get("item").asText());
      assertEquals(999, foundJson.get("price").asInt());
      assertEquals(100, foundJson.get("quantity").asInt());
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for non-TypedDocument")
    void testUpsertWithoutTypedDocument() {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("item", "NewItem");
      objectNode.put("price", 99);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "100");

      assertThrows(
          IllegalArgumentException.class,
          () -> flatCollection.upsert(key, document),
          "Should throw IllegalArgumentException when not using TypedDocument");
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
    @DisplayName("Should throw UnsupportedOperationException for create")
    void testCreate() {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("_id", 300);
      objectNode.put("item", "Brand New Item");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "300");

      assertThrows(UnsupportedOperationException.class, () -> flatCollection.create(key, document));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for createOrReplace")
    void testCreateOrReplace() {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("_id", 200);
      objectNode.put("item", "NewMirror");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "200");

      assertThrows(
          UnsupportedOperationException.class, () -> flatCollection.createOrReplace(key, document));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for createOrReplaceAndReturn")
    void testCreateOrReplaceAndReturn() {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("_id", 200);
      objectNode.put("item", "NewMirror");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "200");

      assertThrows(
          UnsupportedOperationException.class,
          () -> flatCollection.createOrReplaceAndReturn(key, document));
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
