package org.hypertrace.core.documentstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.model.options.ReturnDocumentType;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.hypertrace.core.documentstore.query.Query;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
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

@Testcontainers
public class MongoFlatPgConsistencyTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoFlatPgConsistencyTest.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String COLLECTION_NAME = "consistency_test";
  private static final String DEFAULT_TENANT = "default";
  private static final String MONGO_STORE = "Mongo";
  private static final String POSTGRES_FLAT_STORE = "PostgresFlat";

  private static Map<String, Datastore> datastoreMap;
  private static Map<String, Collection> collectionMap;

  private static GenericContainer<?> mongo;
  private static GenericContainer<?> postgres;

  @BeforeAll
  public static void init() throws IOException {
    datastoreMap = new HashMap<>();
    collectionMap = new HashMap<>();

    // Start MongoDB
    mongo =
        new GenericContainer<>(DockerImageName.parse("mongo:8.0.1"))
            .withExposedPorts(27017)
            .waitingFor(Wait.forListeningPort());
    mongo.start();

    Map<String, String> mongoConfig = new HashMap<>();
    mongoConfig.put("host", "localhost");
    mongoConfig.put("port", mongo.getMappedPort(27017).toString());
    Config mongoCfg = ConfigFactory.parseMap(mongoConfig);

    Datastore mongoDatastore = DatastoreProvider.getDatastore("Mongo", mongoCfg);
    datastoreMap.put(MONGO_STORE, mongoDatastore);

    // Start PostgreSQL
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

    Datastore postgresDatastore =
        DatastoreProvider.getDatastore("Postgres", ConfigFactory.parseMap(postgresConfig));
    datastoreMap.put(POSTGRES_FLAT_STORE, postgresDatastore);

    // Create Postgres flat collection schema
    createFlatCollectionSchema((PostgresDatastore) postgresDatastore);

    // Create collections
    mongoDatastore.deleteCollection(COLLECTION_NAME);
    mongoDatastore.createCollection(COLLECTION_NAME, null);
    collectionMap.put(MONGO_STORE, mongoDatastore.getCollection(COLLECTION_NAME));
    collectionMap.put(
        POSTGRES_FLAT_STORE,
        postgresDatastore.getCollectionForType(COLLECTION_NAME, DocumentType.FLAT));

    LOGGER.info("Test setup complete. Collections ready for both Mongo and PostgresFlat.");
  }

  private static void createFlatCollectionSchema(PostgresDatastore pgDatastore) {
    String createTableSQL =
        String.format(
            "CREATE TABLE \"%s\" ("
                + "\"id\" TEXT PRIMARY KEY,"
                + "\"item\" TEXT,"
                + "\"price\" INTEGER,"
                + "\"quantity\" INTEGER,"
                + "\"in_stock\" BOOLEAN,"
                + "\"tags\" TEXT[],"
                + "\"props\" JSONB"
                + ");",
            COLLECTION_NAME);

    try (Connection connection = pgDatastore.getPostgresClient();
        PreparedStatement statement = connection.prepareStatement(createTableSQL)) {
      statement.execute();
      LOGGER.info("Created flat collection table: {}", COLLECTION_NAME);
    } catch (Exception e) {
      LOGGER.error("Failed to create flat collection schema: {}", e.getMessage(), e);
      throw new RuntimeException("Failed to create flat collection schema", e);
    }
  }

  @BeforeEach
  public void clearCollections() {
    Collection mongoCollection = collectionMap.get(MONGO_STORE);
    mongoCollection.deleteAll();

    PostgresDatastore pgDatastore = (PostgresDatastore) datastoreMap.get(POSTGRES_FLAT_STORE);
    String deleteSQL = String.format("DELETE FROM \"%s\"", COLLECTION_NAME);
    try (Connection connection = pgDatastore.getPostgresClient();
        PreparedStatement statement = connection.prepareStatement(deleteSQL)) {
      statement.executeUpdate();
    } catch (Exception e) {
      LOGGER.error("Failed to clear Postgres table: {}", e.getMessage(), e);
    }
  }

  @AfterAll
  public static void shutdown() {
    if (mongo != null) {
      mongo.stop();
    }
    if (postgres != null) {
      postgres.stop();
    }
  }

  private static class AllStoresProvider implements ArgumentsProvider {

    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(Arguments.of(MONGO_STORE), Arguments.of(POSTGRES_FLAT_STORE));
    }
  }

  private Collection getCollection(String storeName) {
    return collectionMap.get(storeName);
  }

  private static String generateDocId(String prefix) {
    return prefix + "-" + System.currentTimeMillis() + "-" + (int) (Math.random() * 10000);
  }

  private static String getKeyString(String docId) {
    return new SingleValueKey(DEFAULT_TENANT, docId).toString();
  }

  private Query buildQueryById(String docId) {
    return Query.builder()
        .setFilter(
            RelationalExpression.of(
                IdentifierExpression.of("id"),
                RelationalOperator.EQ,
                ConstantExpression.of(getKeyString(docId))))
        .build();
  }

  private void insertMinimalTestDocument(String docId) throws IOException {
    Key key = new SingleValueKey(DEFAULT_TENANT, docId);
    String keyStr = key.toString();

    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put("id", keyStr);
    objectNode.put("item", "Minimal Item");

    Document document = new JSONDocument(objectNode);

    for (Collection collection : collectionMap.values()) {
      collection.upsert(key, document);
    }
  }

  @Nested
  @DisplayName("SubDocument Compatibility Tests")
  class SubDocCompatibilityTest {

    @Nested
    @DisplayName(
        "Non-Existent Fields in JSONB Column. Subdoc updates on non-existent JSONB fields should create those fields in both Mongo and PG")
    class JsonbNonExistentFieldTests {

      @ParameterizedTest(name = "{0}: SET on non-existent nested field should create field")
      @ArgumentsSource(AllStoresProvider.class)
      void testSet(String storeName) throws Exception {
        String docId = generateDocId("set-nonexistent");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);

        Query query = buildQueryById(docId);

        // SET props.brand which doesn't exist
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("props.brand")
                    .operator(UpdateOperator.SET)
                    .subDocumentValue(SubDocumentValue.of("NewBrand"))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent(), storeName + ": Should return updated document");
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        JsonNode propsNode = resultJson.get("props");
        assertNotNull(propsNode, storeName + ": props should be created");
        assertEquals(
            "NewBrand", propsNode.get("brand").asText(), storeName + ": brand should be set");
      }

      @ParameterizedTest(name = "{0}: ADD on non-existent nested field behavior")
      @ArgumentsSource(AllStoresProvider.class)
      void testAdd(String storeName) throws Exception {
        String docId = generateDocId("add-nonexistent");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);

        Query query = buildQueryById(docId);

        // ADD to props.count which doesn't exist
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("props.count")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(10))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent(), storeName + ": Should return updated document");
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        // ADD on non-existent field should treat it as 0 and add, resulting in the value
        JsonNode propsNode = resultJson.get("props");
        assertNotNull(propsNode, storeName + ": props should be created");
        assertEquals(
            10, propsNode.get("count").asInt(), storeName + ": count should be 10 (0 + 10)");
      }

      @ParameterizedTest(name = "{0}: UNSET on non-existent nested field behavior")
      @ArgumentsSource(AllStoresProvider.class)
      void testUnset(String storeName) throws Exception {
        String docId = generateDocId("unset-nonexistent");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);

        Query query = buildQueryById(docId);

        // UNSET props.brand which doesn't exist
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("props.brand")
                    .operator(UpdateOperator.UNSET)
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        // Should succeed without error - UNSET on non-existent is a no-op
        assertTrue(result.isPresent(), storeName + ": Should return updated document");
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        // Document should still exist with original fields
        assertEquals("Minimal Item", resultJson.get("item").asText());
      }

      @ParameterizedTest(name = "{0}: APPEND_TO_LIST on non-existent nested array behavior")
      @ArgumentsSource(AllStoresProvider.class)
      void testAppendToList(String storeName) throws Exception {
        String docId = generateDocId("append-nonexistent");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);

        Query query = buildQueryById(docId);

        // APPEND_TO_LIST on props.colors which doesn't exist
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("props.colors")
                    .operator(UpdateOperator.APPEND_TO_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[]{"red", "blue"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent(), storeName + ": Should return updated document");
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        // Should create the array with the appended values
        JsonNode propsNode = resultJson.get("props");
        assertNotNull(propsNode, storeName + ": props should be created");
        JsonNode colorsNode = propsNode.get("colors");
        assertNotNull(colorsNode, storeName + ": colors should be created");
        assertTrue(colorsNode.isArray(), storeName + ": colors should be an array");
        assertEquals(2, colorsNode.size(), storeName + ": colors should have 2 elements");
      }

      @ParameterizedTest(name = "{0}: ADD_TO_LIST_IF_ABSENT on non-existent nested array behavior")
      @ArgumentsSource(AllStoresProvider.class)
      void testAddToListIfAbsent(String storeName) throws Exception {
        String docId = generateDocId("addifabsent-nonexistent");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);

        Query query = buildQueryById(docId);

        // ADD_TO_LIST_IF_ABSENT on props.tags which doesn't exist
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("props.tags")
                    .operator(UpdateOperator.ADD_TO_LIST_IF_ABSENT)
                    .subDocumentValue(SubDocumentValue.of(new String[]{"tag1", "tag2"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent(), storeName + ": Should return updated document");
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        // Should create the array with the values
        JsonNode propsNode = resultJson.get("props");
        assertNotNull(propsNode, storeName + ": props should be created");
        JsonNode tagsNode = propsNode.get("tags");
        assertNotNull(tagsNode, storeName + ": tags should be created");
        assertTrue(tagsNode.isArray(), storeName + ": tags should be an array");
        assertEquals(2, tagsNode.size(), storeName + ": tags should have 2 elements");
      }

      @ParameterizedTest(name = "{0}: REMOVE_ALL_FROM_LIST on non-existent nested array behavior")
      @ArgumentsSource(AllStoresProvider.class)
      void testRemoveAllFromList(String storeName) throws Exception {
        String docId = generateDocId("removeall-nonexistent");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);

        Query query = buildQueryById(docId);

        // REMOVE_ALL_FROM_LIST on props.colors which doesn't exist
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("props.colors")
                    .operator(UpdateOperator.REMOVE_ALL_FROM_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[]{"red"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        // Should succeed - removing from non-existent list is a no-op or results in empty array
        assertTrue(result.isPresent(), storeName + ": Should return updated document");
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        // Document should still exist
        assertEquals("Minimal Item", resultJson.get("item").asText());
      }

      @ParameterizedTest(name = "{0}: SET on deep nested path should create intermediate objects")
      @ArgumentsSource(AllStoresProvider.class)
      void testSetDeepNested(String storeName) throws Exception {
        String docId = generateDocId("set-deep");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // SET props.brand.category.name - all intermediate objects don't exist
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("props.brand.category.name")
                    .operator(UpdateOperator.SET)
                    .subDocumentValue(SubDocumentValue.of("Electronics"))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        // Verify deep nested structure was created
        JsonNode propsNode = resultJson.get("props");
        assertNotNull(propsNode, storeName + ": props should be created");
        JsonNode brandNode = propsNode.get("brand");
        assertNotNull(brandNode, storeName + ": props.brand should be created");
        JsonNode categoryNode = brandNode.get("category");
        assertNotNull(categoryNode, storeName + ": props.brand.category should be created");
        assertEquals("Electronics", categoryNode.get("name").asText());
      }

      @ParameterizedTest(name = "{0}: ADD on deep nested path should create intermediate objects")
      @ArgumentsSource(AllStoresProvider.class)
      void testAddDeepNested(String storeName) throws Exception {
        String docId = generateDocId("add-deep");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // ADD to props.stats.sales.count - all intermediate objects don't exist
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("props.stats.sales.count")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(5))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        JsonNode propsNode = resultJson.get("props");
        assertNotNull(propsNode, storeName + ": props should be created");
        JsonNode statsNode = propsNode.get("stats");
        assertNotNull(statsNode, storeName + ": props.stats should be created");
        JsonNode salesNode = statsNode.get("sales");
        assertNotNull(salesNode, storeName + ": props.stats.sales should be created");
        assertEquals(5, salesNode.get("count").asInt());
      }

      @ParameterizedTest(name = "{0}: APPEND_TO_LIST on deep nested path should create intermediate objects")
      @ArgumentsSource(AllStoresProvider.class)
      void testAppendToListDeepNested(String storeName) throws Exception {
        String docId = generateDocId("append-deep");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // APPEND_TO_LIST to props.metadata.tags.items - all intermediate objects don't exist
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("props.metadata.tags.items")
                    .operator(UpdateOperator.APPEND_TO_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[]{"tag1", "tag2"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        JsonNode propsNode = resultJson.get("props");
        assertNotNull(propsNode);
        JsonNode metadataNode = propsNode.get("metadata");
        assertNotNull(metadataNode);
        JsonNode tagsNode = metadataNode.get("tags");
        assertNotNull(tagsNode);
        JsonNode itemsNode = tagsNode.get("items");
        assertNotNull(itemsNode);
        assertTrue(itemsNode.isArray());
        assertEquals(2, itemsNode.size());
      }
    }

    @Nested
    @DisplayName("Top-Level Fields Not In PG Schema (Mongo creates, PG skips)")
    class TopLevelSchemaMissingFieldTests {

      @ParameterizedTest(name = "{0}: SET on field not in PG schema")
      @ArgumentsSource(AllStoresProvider.class)
      void testSet(String storeName) throws Exception {
        String docId = generateDocId("set-schema-missing");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // SET unknownField which doesn't exist in PG schema
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("unknownField")
                    .operator(UpdateOperator.SET)
                    .subDocumentValue(SubDocumentValue.of("newValue"))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent(), storeName + ": Should return updated document");
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        if (MONGO_STORE.equals(storeName)) {
          // Mongo creates the field
          assertNotNull(
              resultJson.get("unknownField"), storeName + ": unknownField should be created");
          assertEquals("newValue", resultJson.get("unknownField").asText());
        } else {
          // Postgres SKIP strategy: field not created, no-op
          assertTrue(
              resultJson.get("unknownField") == null || resultJson.get("unknownField").isNull());
        }
      }

      @ParameterizedTest(name = "{0}: ADD on field not in PG schema")
      @ArgumentsSource(AllStoresProvider.class)
      void testAdd(String storeName) throws Exception {
        String docId = generateDocId("add-schema-missing");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // ADD to unknownCount which doesn't exist in PG schema
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("unknownCount")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(10))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent(), storeName + ": Should return updated document");
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        if (MONGO_STORE.equals(storeName)) {
          // Mongo creates the field with value
          assertNotNull(
              resultJson.get("unknownCount"), storeName + ": unknownCount should be created");
          assertEquals(10, resultJson.get("unknownCount").asInt());
        } else {
          // Postgres SKIP strategy: field not created, no-op
          assertTrue(
              resultJson.get("unknownCount") == null || resultJson.get("unknownCount").isNull());
        }
      }

      @ParameterizedTest(name = "{0}: UNSET on field not in PG schema")
      @ArgumentsSource(AllStoresProvider.class)
      void testUnset(String storeName) throws Exception {
        String docId = generateDocId("unset-schema-missing");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // UNSET unknownField which doesn't exist in schema or document
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("unknownField")
                    .operator(UpdateOperator.UNSET)
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        // Both Mongo and Postgres: UNSET on non-existent field is a no-op
        assertTrue(result.isPresent(), storeName + ": Should return updated document");
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        assertEquals("Minimal Item", resultJson.get("item").asText());
      }

      @ParameterizedTest(name = "{0}: APPEND_TO_LIST on field not in PG schema")
      @ArgumentsSource(AllStoresProvider.class)
      void testAppendToList(String storeName) throws Exception {
        String docId = generateDocId("append-schema-missing");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // APPEND_TO_LIST on unknownList which doesn't exist in PG schema
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("unknownList")
                    .operator(UpdateOperator.APPEND_TO_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[]{"item1", "item2"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        JsonNode unknownList = resultJson.get("unknownList");
        if (MONGO_STORE.equals(storeName)) {
          // Mongo creates the array
          assertNotNull(unknownList);
          assertTrue(unknownList.isArray());
          assertEquals(2, unknownList.size());
        } else {
          // Postgres SKIP strategy: field not created, no-op
          assertTrue(unknownList == null || unknownList.isNull());
        }
      }

      @ParameterizedTest(name = "{0}: ADD_TO_LIST_IF_ABSENT on field not in PG schema")
      @ArgumentsSource(AllStoresProvider.class)
      void testAddToList(String storeName) throws Exception {
        String docId = generateDocId("addifabsent-schema-missing");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // ADD_TO_LIST_IF_ABSENT on unknownSet which doesn't exist in PG schema
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("unknownSet")
                    .operator(UpdateOperator.ADD_TO_LIST_IF_ABSENT)
                    .subDocumentValue(SubDocumentValue.of(new String[]{"val1", "val2"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        JsonNode unknownSet = resultJson.get("unknownSet");
        if (MONGO_STORE.equals(storeName)) {
          // Mongo creates the array
          assertNotNull(unknownSet);
          assertTrue(unknownSet.isArray());
          assertEquals(2, unknownSet.size());
        } else {
          // Postgres SKIP strategy: field not created, no-op
          assertTrue(unknownSet == null || unknownSet.isNull());
        }
      }

      @ParameterizedTest(name = "{0}: REMOVE_ALL_FROM_LIST on field not in PG schema")
      @ArgumentsSource(AllStoresProvider.class)
      void testRemoveAllFromList(String storeName) throws Exception {
        String docId = generateDocId("removeall-schema-missing");
        insertMinimalTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // REMOVE_ALL_FROM_LIST on unknownList which doesn't exist in schema or document
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("unknownList")
                    .operator(UpdateOperator.REMOVE_ALL_FROM_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[]{"item1"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        // Both Mongo and Postgres: REMOVE_ALL from non-existent is a no-op
        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        assertEquals("Minimal Item", resultJson.get("item").asText());
      }
    }
  }
}
