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
import java.util.Map;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.junit.jupiter.api.AfterAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
    flatCollection = postgresDatastore.getCollectionForType(FLAT_COLLECTION_NAME,
        DocumentType.FLAT);
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
    flatCollection.deleteAll();
    long count = flatCollection.count();
    assertEquals(0, count, "Precondition failure: DB is not empty");
    executeInsertStatements();
    assertEquals(INITIAL_ROW_COUNT, flatCollection.count(),
        "Precondition failure: Not all rows were written into the DB correctly");
  }

  @AfterEach
  public void cleanup() {
    // Data is cleared in @BeforeEach, but cleanup here for safety
  }

  @AfterAll
  public static void shutdown() {
    postgres.stop();
  }

  @Test
  public void testUpsertNewDocument() throws IOException {
    // Initial count
    assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

    // Upsert a new document with ID 100 (not in initial data)
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

    // Should have one more document
    assertEquals(INITIAL_ROW_COUNT + 1, flatCollection.count());
  }

  @Test
  public void testUpsertUpdatesExistingDocument() throws IOException {
    // Initial count
    assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

    // Upsert to update existing document with ID 1
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put("_id", 1);
    objectNode.put("item", "Soap Premium");
    objectNode.put("price", 25);
    objectNode.put("quantity", 100);
    Document document = new JSONDocument(objectNode);

    Key key = new SingleValueKey("default", "1");
    flatCollection.upsert(key, document);

    // Count should remain the same (update, not insert)
    assertEquals(INITIAL_ROW_COUNT, flatCollection.count());
  }

  @Test
  public void testCreateOrReplace() throws IOException {
    assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

    // Create new document with ID 200
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put("_id", 200);
    objectNode.put("item", "NewMirror");
    objectNode.put("price", 30);
    Document document = new JSONDocument(objectNode);

    Key key = new SingleValueKey("default", "200");

    // First call should create (returns true)
    boolean created = flatCollection.createOrReplace(key, document);
    assertTrue(created);
    assertEquals(INITIAL_ROW_COUNT + 1, flatCollection.count());

    // Second call should replace (returns false)
    ObjectNode updatedNode = OBJECT_MAPPER.createObjectNode();
    updatedNode.put("_id", 200);
    updatedNode.put("item", "Mirror Deluxe");
    updatedNode.put("price", 50);
    Document updatedDocument = new JSONDocument(updatedNode);

    boolean createdAgain = flatCollection.createOrReplace(key, updatedDocument);
    assertFalse(createdAgain);
    assertEquals(INITIAL_ROW_COUNT + 1, flatCollection.count());
  }

  @Test
  public void testCreate() throws IOException {
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
  public void testBulkUpsert() {
    assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

    Map<Key, Document> bulkMap = new HashMap<>();

    // Add 5 new documents with IDs 101-105
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

  @Test
  public void testDeleteByKey() {
    assertEquals(INITIAL_ROW_COUNT, flatCollection.count(), getPreconditionFailureMessage(
        "DB does not have initial row count of: " + INITIAL_ROW_COUNT));

    // Delete document with ID 1 (exists in initial data)
    Key keyToDelete = new SingleValueKey("default", "1");
    boolean deleted = flatCollection.delete(keyToDelete);
    assertTrue(deleted);

    assertEquals(INITIAL_ROW_COUNT - 1, flatCollection.count());
  }

  @Test
  public void testDeleteByFilter() throws IOException {
    assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

    // Delete documents where item = "Soap" (IDs 1, 5, 8 in initial data)
    Filter filter = Filter.eq("item", "Soap");
    boolean deleted = flatCollection.delete(filter);
    assertTrue(deleted);

    // Should have 7 remaining
    assertEquals(7, flatCollection.count());
  }

  @Test
  public void testDeleteAll() throws IOException {
    assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

    boolean deleted = flatCollection.deleteAll();
    assertTrue(deleted);

    assertEquals(0, flatCollection.count());
  }

  @Test
  public void testCount() throws IOException {
    assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

    // Add more documents
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

  @Test
  public void testUpdateWithCondition() throws IOException {
    assertEquals(INITIAL_ROW_COUNT, flatCollection.count());

    // Update document with ID 1 where price = 10
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

  @Test
  public void testUpsertWithJsonbField() throws IOException {
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

  private static String getPreconditionFailureMessage(String message) {
    return String.format("%s: %s", "Precondition failure:", message);
  }
}
