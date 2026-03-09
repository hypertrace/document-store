package org.hypertrace.core.documentstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.testcontainers.junit.jupiter.Testcontainers;

/*Validates write consistency b/w Mongo and Postgres*/
@Testcontainers
public class MongoPostgresWriteConsistencyTest extends BaseWriteTest {

  private static final String COLLECTION_NAME = "consistency_test";
  private static final String MONGO_STORE = "Mongo";
  private static final String POSTGRES_FLAT_STORE = "PostgresFlat";

  @BeforeAll
  public static void init() throws IOException {
    // Start MongoDB and PostgreSQL using BaseWriteTest setup
    initMongo();
    initPostgres();

    datastoreMap.put(MONGO_STORE, mongoDatastore);
    datastoreMap.put(POSTGRES_FLAT_STORE, postgresDatastore);

    // Create Postgres flat collection schema
    createFlatCollectionSchema((PostgresDatastore) postgresDatastore, COLLECTION_NAME);

    // Create collections
    mongoDatastore.deleteCollection(COLLECTION_NAME);
    mongoDatastore.createCollection(COLLECTION_NAME, null);
    collectionMap.put(MONGO_STORE, mongoDatastore.getCollection(COLLECTION_NAME));
    collectionMap.put(
        POSTGRES_FLAT_STORE,
        postgresDatastore.getCollectionForType(COLLECTION_NAME, DocumentType.FLAT));

    LOGGER.info("Test setup complete. Collections ready for both Mongo and PostgresFlat.");
  }

  @BeforeEach
  public void clearCollections() {
    collectionMap.get(MONGO_STORE).deleteAll();
    clearTable(COLLECTION_NAME);
  }

  @AfterAll
  public static void shutdown() {
    shutdownMongo();
    shutdownPostgres();
  }

  private static class AllStoresProvider implements ArgumentsProvider {

    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(Arguments.of(MONGO_STORE), Arguments.of(POSTGRES_FLAT_STORE));
    }
  }

  /** Inserts a test document into all collections (Mongo and PG) */
  private void insertTestDocument(String docId) throws IOException {
    for (Collection collection : collectionMap.values()) {
      insertTestDocument(docId, collection);
    }
  }

  @Nested
  @DisplayName("Upsert Consistency Tests")
  class UpsertConsistencyTests {

    @ParameterizedTest(name = "{0}: upsert with all field types")
    @ArgumentsSource(AllStoresProvider.class)
    void testUpsertNewDoc(String storeName) throws Exception {
      String docId = generateDocId("upsert-all");
      Key key = createKey(docId);

      Collection collection = getCollection(storeName);

      // Create document with all field types
      Document document = createTestDocument(docId);
      boolean isNew = collection.upsert(key, document);
      assertTrue(isNew);

      // Verify by upserting again (returns true again if the operation succeeds)
      boolean secondUpsert = collection.upsert(key, document);
      assertTrue(secondUpsert);

      // Query the collection to get the document back
      Query query = buildQueryById(docId);
      try (CloseableIterator<Document> iterator = collection.find(query)) {
        assertTrue(iterator.hasNext());
        Document retrievedDoc = iterator.next();
        JsonNode resultJson = OBJECT_MAPPER.readTree(retrievedDoc.toJson());

        // Verify primitives
        assertEquals("TestItem", resultJson.get("item").asText(), storeName);
        assertEquals(100, resultJson.get("price").asInt(), storeName);
        assertEquals(50, resultJson.get("quantity").asInt(), storeName);
        assertTrue(resultJson.get("in_stock").asBoolean(), storeName);
        assertEquals(1000000000000L, resultJson.get("big_number").asLong(), storeName);
        assertEquals(3.5, resultJson.get("rating").asDouble(), 0.01, storeName);
        assertEquals(50.0, resultJson.get("weight").asDouble(), 0.01, storeName);

        // Verify arrays
        JsonNode tagsNode = resultJson.get("tags");
        assertNotNull(tagsNode);
        assertTrue(tagsNode.isArray(), storeName);
        assertEquals(2, tagsNode.size(), storeName);
        assertEquals("tag1", tagsNode.get(0).asText(), storeName);
        assertEquals("tag2", tagsNode.get(1).asText(), storeName);

        JsonNode numbersNode = resultJson.get("numbers");
        assertNotNull(numbersNode);
        assertTrue(numbersNode.isArray(), storeName);
        assertEquals(3, numbersNode.size(), storeName);

        // Verify JSONB - props
        JsonNode propsNode = resultJson.get("props");
        assertNotNull(propsNode);
        assertEquals("TestBrand", propsNode.get("brand").asText(), storeName);
        assertEquals("M", propsNode.get("size").asText(), storeName);
        assertEquals(10, propsNode.get("count").asInt(), storeName);
        JsonNode colorsNode = propsNode.get("colors");
        assertTrue(colorsNode.isArray(), storeName);
        assertEquals(2, colorsNode.size(), storeName);

        // Verify JSONB - sales
        JsonNode salesNode = resultJson.get("sales");
        assertNotNull(salesNode);
        assertEquals(200, salesNode.get("total").asInt(), storeName);
        assertEquals(10, salesNode.get("count").asInt(), storeName);
      }
    }

    @ParameterizedTest(name = "{0}: upsert preserves existing values (merge behavior)")
    @ArgumentsSource(AllStoresProvider.class)
    void testUpsertExistingDoc(String storeName) throws Exception {
      String docId = generateDocId("upsert-merge");
      Key key = createKey(docId);

      Collection collection = getCollection(storeName);

      Document initialDoc = createTestDocument(docId);
      collection.upsert(key, initialDoc);

      ObjectNode partialNode = OBJECT_MAPPER.createObjectNode();
      partialNode.put("id", getKeyString(docId));
      partialNode.put("item", "UpdatedItem");
      partialNode.put("price", 999);
      Document partialDoc = new JSONDocument(partialNode);

      collection.upsert(key, partialDoc);

      Query query = buildQueryById(docId);
      try (CloseableIterator<Document> iterator = collection.find(query)) {
        assertTrue(iterator.hasNext());
        Document retrievedDoc = iterator.next();
        JsonNode resultJson = OBJECT_MAPPER.readTree(retrievedDoc.toJson());

        // Updated fields
        assertEquals(
            "UpdatedItem", resultJson.get("item").asText(), storeName + ": item should be updated");
        assertEquals(999, resultJson.get("price").asInt(), storeName + ": price should be updated");

        // Non-updated fields
        assertEquals(
            50, resultJson.get("quantity").asInt(), storeName + ": quantity should be preserved");
        assertTrue(
            resultJson.get("in_stock").asBoolean(), storeName + ": in_stock should be preserved");
        assertEquals(
            1000000000000L,
            resultJson.get("big_number").asLong(),
            storeName + ": big_number should be preserved");
        assertEquals(
            3.5,
            resultJson.get("rating").asDouble(),
            0.01,
            storeName + ": rating should be preserved");

        JsonNode tagsNode = resultJson.get("tags");
        assertNotNull(tagsNode, storeName + ": tags should be preserved");
        assertEquals(2, tagsNode.size(), storeName);

        JsonNode propsNode = resultJson.get("props");
        assertNotNull(propsNode, storeName + ": props should be preserved");
        assertEquals("TestBrand", propsNode.get("brand").asText(), storeName);

        JsonNode salesNode = resultJson.get("sales");
        assertNotNull(salesNode, storeName + ": sales should be preserved");
        assertEquals(200, salesNode.get("total").asInt(), storeName);
      }
    }

    @ParameterizedTest(name = "{0}: bulkUpsert multiple documents")
    @ArgumentsSource(AllStoresProvider.class)
    void testBulkUpsert(String storeName) throws Exception {
      String docId1 = generateDocId("bulk-1");
      String docId2 = generateDocId("bulk-2");

      Collection collection = getCollection(storeName);

      Map<Key, Document> documents = new HashMap<>();
      documents.put(createKey(docId1), createTestDocument(docId1));
      documents.put(createKey(docId2), createTestDocument(docId2));

      boolean result = collection.bulkUpsert(documents);
      assertTrue(result);

      for (String docId : List.of(docId1, docId2)) {
        Query query = buildQueryById(docId);
        try (CloseableIterator<Document> iterator = collection.find(query)) {
          assertTrue(iterator.hasNext());
          Document doc = iterator.next();
          JsonNode json = OBJECT_MAPPER.readTree(doc.toJson());

          assertEquals("TestItem", json.get("item").asText(), storeName);
          assertEquals(100, json.get("price").asInt(), storeName);
          assertEquals(50, json.get("quantity").asInt(), storeName);
          assertTrue(json.get("in_stock").asBoolean(), storeName);

          JsonNode tagsNode = json.get("tags");
          assertNotNull(tagsNode, storeName);
          assertEquals(2, tagsNode.size(), storeName);
        }
      }
    }

    @ParameterizedTest(name = "{0}: upsert with non-existing fields (schema mismatch)")
    @ArgumentsSource(AllStoresProvider.class)
    void testUpsertNonExistingFields(String storeName) throws Exception {
      String docId = generateDocId("upsert-unknown");
      Key key = createKey(docId);

      Collection collection = getCollection(storeName);

      // Create document with fields that don't exist in the PG schema
      ObjectNode docNode = OBJECT_MAPPER.createObjectNode();
      docNode.put("id", getKeyString(docId));
      docNode.put("item", "TestItem");
      docNode.put("price", 100);
      docNode.put("unknown_field_1", "unknown_value");
      docNode.put("unknown_field_2", 999);
      Document document = new JSONDocument(docNode);

      // Upsert should succeed (PG skips unknown fields with default strategy)
      boolean result = collection.upsert(key, document);
      assertTrue(result);

      // Verify document exists with known fields
      Query query = buildQueryById(docId);
      try (CloseableIterator<Document> iterator = collection.find(query)) {
        assertTrue(iterator.hasNext());
        Document retrievedDoc = iterator.next();
        JsonNode json = OBJECT_MAPPER.readTree(retrievedDoc.toJson());

        // Known fields should exist
        assertEquals("TestItem", json.get("item").asText(), storeName);
        assertEquals(100, json.get("price").asInt(), storeName);

        // For Mongo, unknown fields will be stored; for PG with SKIP strategy, they won't
        if (storeName.equals("Mongo")) {
          assertNotNull(json.get("unknown_field_1"));
          assertEquals("unknown_value", json.get("unknown_field_1").asText());
          assertNotNull(json.get("unknown_field_2"));
          assertEquals(999, json.get("unknown_field_2").asInt());
        }
      }
    }
  }

  @Nested
  @DisplayName("CreateOrReplace Consistency Tests")
  class CreateOrReplaceConsistencyTests {

    @ParameterizedTest(name = "{0}: createOrReplace new document")
    @ArgumentsSource(AllStoresProvider.class)
    void testCreateOrReplaceNewDoc(String storeName) throws Exception {
      String docId = generateDocId("cor-new");
      Key key = createKey(docId);

      Collection collection = getCollection(storeName);

      Document document = createTestDocument(docId);
      boolean isNew = collection.createOrReplace(key, document);
      assertTrue(isNew);

      Query query = buildQueryById(docId);
      try (CloseableIterator<Document> iterator = collection.find(query)) {
        assertTrue(iterator.hasNext());
        Document retrievedDoc = iterator.next();
        JsonNode json = OBJECT_MAPPER.readTree(retrievedDoc.toJson());

        assertEquals("TestItem", json.get("item").asText(), storeName);
        assertEquals(100, json.get("price").asInt(), storeName);
        assertEquals(50, json.get("quantity").asInt(), storeName);
      }
    }

    @ParameterizedTest(name = "{0}: createOrReplace replaces entire document")
    @ArgumentsSource(AllStoresProvider.class)
    void testCreateOrReplaceExistingDoc(String storeName) throws Exception {
      String docId = generateDocId("cor-replace");
      Key key = createKey(docId);

      Collection collection = getCollection(storeName);

      // First create with all fields
      Document initialDoc = createTestDocument(docId);
      collection.createOrReplace(key, initialDoc);

      // Replace with partial document - unlike upsert, this should REPLACE entirely
      ObjectNode replacementNode = OBJECT_MAPPER.createObjectNode();
      replacementNode.put("id", getKeyString(docId));
      replacementNode.put("item", "ReplacedItem");
      replacementNode.put("price", 777);
      // Note: quantity, in_stock, tags, props, sales are NOT specified
      Document replacementDoc = new JSONDocument(replacementNode);

      boolean isNew = collection.createOrReplace(key, replacementDoc);
      assertFalse(isNew);

      Query query = buildQueryById(docId);
      try (CloseableIterator<Document> iterator = collection.find(query)) {
        assertTrue(iterator.hasNext());
        Document retrievedDoc = iterator.next();
        JsonNode json = OBJECT_MAPPER.readTree(retrievedDoc.toJson());

        // Replaced fields should have new values
        assertEquals("ReplacedItem", json.get("item").asText(), storeName);
        assertEquals(777, json.get("price").asInt(), storeName);

        // Note that PG should return null for non-specified fields. However, the iterator
        // specifically excludes null fields
        // from the result set, so we expect these to be missing.
        assertNull(json.get("quantity"));
        assertNull(json.get("in_stock"));
        assertNull(json.get("tags"));
        assertNull(json.get("props"));
        assertNull(json.get("sales"));
      }
    }

    @ParameterizedTest(name = "{0}: createOrReplaceAndReturn")
    @ArgumentsSource(AllStoresProvider.class)
    @Disabled("Not implemented for PG")
    void testCreateOrReplaceAndReturn(String storeName) throws Exception {
      String docId = generateDocId("cor-return");
      Key key = createKey(docId);

      Collection collection = getCollection(storeName);

      Document document = createTestDocument(docId);
      Document returned = collection.createOrReplaceAndReturn(key, document);

      assertNotNull(returned);
      JsonNode json = OBJECT_MAPPER.readTree(returned.toJson());

      assertEquals("TestItem", json.get("item").asText(), storeName);
      assertEquals(100, json.get("price").asInt(), storeName);
      assertEquals(50, json.get("quantity").asInt(), storeName);
    }
  }

  @Nested
  class SubdocUpdateConsistencyTests {

    @Nested
    @DisplayName("SET Operator Tests")
    class SetOperatorTests {

      @ParameterizedTest(name = "{0}: SET top-level primitives")
      @ArgumentsSource(AllStoresProvider.class)
      void testSetTopLevelPrimitives(String storeName) throws Exception {
        String docId = generateDocId("set-primitives");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.of("item", "UpdatedItem"),
                SubDocumentUpdate.of("price", 999),
                SubDocumentUpdate.of("quantity", 50),
                SubDocumentUpdate.of("in_stock", false),
                SubDocumentUpdate.of("big_number", 9999999999L),
                SubDocumentUpdate.of("rating", 4.5f),
                SubDocumentUpdate.of("weight", 123.456));

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        assertEquals("UpdatedItem", resultJson.get("item").asText(), storeName);
        assertEquals(999, resultJson.get("price").asInt(), storeName);
        assertFalse(resultJson.get("in_stock").asBoolean(), storeName);
        assertEquals(9999999999L, resultJson.get("big_number").asLong(), storeName);
        assertEquals(4.5, resultJson.get("rating").asDouble(), 0.01, storeName);
        assertEquals(123.456, resultJson.get("weight").asDouble(), 0.01, storeName);
      }

      @ParameterizedTest(name = "{0}: SET top-level array")
      @ArgumentsSource(AllStoresProvider.class)
      void testSetTopLevelArray(String storeName) throws Exception {
        String docId = generateDocId("set-array");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        List<SubDocumentUpdate> updates =
            List.of(SubDocumentUpdate.of("tags", new String[] {"tag4", "tag5", "tag6"}));

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        JsonNode tagsNode = resultJson.get("tags");
        assertTrue(tagsNode.isArray());
        assertEquals(3, tagsNode.size(), storeName);
        assertEquals("tag4", tagsNode.get(0).asText());
        assertEquals("tag5", tagsNode.get(1).asText());
        assertEquals("tag6", tagsNode.get(2).asText());
      }

      @ParameterizedTest(name = "{0}: SET top-level array")
      @ArgumentsSource(AllStoresProvider.class)
      void testSetTopLevelEmptyArray(String storeName) throws Exception {
        String docId = generateDocId("set-array");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("tags", new String[] {}));

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        JsonNode tagsNode = resultJson.get("tags");
        assertTrue(tagsNode.isArray());
        assertEquals(0, tagsNode.size(), storeName);
      }

      @ParameterizedTest(name = "{0}: SET nested JSONB primitive")
      @ArgumentsSource(AllStoresProvider.class)
      void testSetNestedJsonbPrimitive(String storeName) throws Exception {
        String docId = generateDocId("set-nested");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

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

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        assertEquals("NewBrand", resultJson.get("props").get("brand").asText(), storeName);
        // Other props fields preserved
        assertEquals("M", resultJson.get("props").get("size").asText(), storeName);
        assertEquals(10, resultJson.get("props").get("count").asInt(), storeName);
      }

      @ParameterizedTest(name = "{0}: SET nested JSONB array")
      @ArgumentsSource(AllStoresProvider.class)
      void testSetNestedJsonbArray(String storeName) throws Exception {
        String docId = generateDocId("set-nested-array");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("sales.regions")
                    .operator(UpdateOperator.SET)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"US", "EU", "APAC"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        JsonNode regionsNode = resultJson.get("sales").get("regions");
        assertTrue(regionsNode.isArray());
        assertEquals(3, regionsNode.size(), storeName);
        // Other sales fields preserved
        assertEquals(200, resultJson.get("sales").get("total").asInt(), storeName);
      }
    }

    @Nested
    @DisplayName("UNSET Operator Tests")
    class UnsetOperatorTests {

      @ParameterizedTest(name = "{0}: UNSET top-level column and nested JSONB field")
      @ArgumentsSource(AllStoresProvider.class)
      void testUnsetTopLevelAndNestedFields(String storeName) throws Exception {
        String docId = generateDocId("unset");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        List<SubDocumentUpdate> updates =
            List.of(
                // Top-level: sets column to NULL
                SubDocumentUpdate.builder()
                    .subDocument("item")
                    .operator(UpdateOperator.UNSET)
                    .build(),
                // Nested JSONB: removes key from JSON object
                SubDocumentUpdate.builder()
                    .subDocument("props.brand")
                    .operator(UpdateOperator.UNSET)
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        // Verify top-level column is NULL/missing
        JsonNode itemNode = resultJson.get("item");
        assertTrue(itemNode == null || itemNode.isNull(), storeName + ": item should be unset");

        // Verify nested JSONB key is removed, but other keys preserved
        assertFalse(
            resultJson.get("props").has("brand"), storeName + ": props.brand should be unset");
        assertEquals("M", resultJson.get("props").get("size").asText(), storeName);
      }
    }

    @Nested
    @DisplayName("ADD Operator Tests")
    class AddOperatorTests {

      @ParameterizedTest(name = "{0}: ADD to all numeric types")
      @ArgumentsSource(AllStoresProvider.class)
      void testAddAllNumericTypes(String storeName) throws Exception {
        String docId = generateDocId("add-numeric");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        List<SubDocumentUpdate> updates =
            List.of(
                // Top-level INT: 100 + 5 = 105
                SubDocumentUpdate.builder()
                    .subDocument("price")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(5))
                    .build(),
                // Top-level INT (negative): 50 + (-15) = 35
                SubDocumentUpdate.builder()
                    .subDocument("quantity")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(-15))
                    .build(),
                // Top-level BIGINT: 1000000000000 + 500 = 1000000000500
                SubDocumentUpdate.builder()
                    .subDocument("big_number")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(500L))
                    .build(),
                // Top-level REAL: 3.5 + 1.0 = 4.5
                SubDocumentUpdate.builder()
                    .subDocument("rating")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(1.0f))
                    .build(),
                // Top-level DOUBLE: 50.0 + 2.5 = 52.5
                SubDocumentUpdate.builder()
                    .subDocument("weight")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(2.5))
                    .build(),
                // Nested JSONB: 200 + 50 = 250
                SubDocumentUpdate.builder()
                    .subDocument("sales.total")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(50))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        assertEquals(105, resultJson.get("price").asInt(), storeName + ": 100 + 5 = 105");
        assertEquals(35, resultJson.get("quantity").asInt(), storeName + ": 50 + (-15) = 35");
        assertEquals(1000000000500L, resultJson.get("big_number").asLong(), storeName);
        assertEquals(
            4.5, resultJson.get("rating").asDouble(), 0.01, storeName + ": 3.5 + 1.0 = 4.5");
        assertEquals(
            52.5, resultJson.get("weight").asDouble(), 0.01, storeName + ": 50.0 + 2.5 = 52.5");
        assertEquals(
            250, resultJson.get("sales").get("total").asInt(), storeName + ": 200 + 50 = 250");
        // Other fields preserved
        assertEquals(10, resultJson.get("sales").get("count").asInt(), storeName);
      }

      @ParameterizedTest(name = "{0}: ADD on non-numeric field (TEXT column)")
      @ArgumentsSource(AllStoresProvider.class)
      void testAddOnNonNumericField(String storeName) throws Exception {
        String docId = generateDocId("add-non-numeric");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // Try to ADD to 'item' which is a TEXT field
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("item")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(10))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        assertThrows(Exception.class, () -> collection.update(query, updates, options));
      }
    }

    @Nested
    @DisplayName("APPEND_TO_LIST Operator Tests")
    class AppendToListOperatorTests {

      @ParameterizedTest(name = "{0}: APPEND_TO_LIST for top-level and nested arrays")
      @ArgumentsSource(AllStoresProvider.class)
      void testAppendToListAllCases(String storeName) throws Exception {
        String docId = generateDocId("append");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        List<SubDocumentUpdate> updates =
            List.of(
                // Top-level array: append to existing tags
                SubDocumentUpdate.builder()
                    .subDocument("tags")
                    .operator(UpdateOperator.APPEND_TO_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"newTag1", "newTag2"}))
                    .build(),
                // Nested JSONB array: append to existing props.colors
                SubDocumentUpdate.builder()
                    .subDocument("props.colors")
                    .operator(UpdateOperator.APPEND_TO_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"green", "yellow"}))
                    .build(),
                // Nested JSONB: append to non-existent array (creates it)
                SubDocumentUpdate.builder()
                    .subDocument("sales.regions")
                    .operator(UpdateOperator.APPEND_TO_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"US", "EU"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        // Verify top-level array append
        JsonNode tagsNode = resultJson.get("tags");
        assertTrue(tagsNode.isArray());
        assertEquals(4, tagsNode.size(), storeName + ": 2 + 2 = 4 tags");
        assertEquals("tag1", tagsNode.get(0).asText());
        assertEquals("tag2", tagsNode.get(1).asText());
        assertEquals("newTag1", tagsNode.get(2).asText());
        assertEquals("newTag2", tagsNode.get(3).asText());

        // Verify nested JSONB array append
        JsonNode colorsNode = resultJson.get("props").get("colors");
        assertTrue(colorsNode.isArray());
        assertEquals(4, colorsNode.size(), storeName + ": 2 + 2 = 4 colors");
        assertEquals("red", colorsNode.get(0).asText());
        assertEquals("blue", colorsNode.get(1).asText());
        assertEquals("green", colorsNode.get(2).asText());
        assertEquals("yellow", colorsNode.get(3).asText());

        // Verify non-existent array was created
        JsonNode regionsNode = resultJson.get("sales").get("regions");
        assertNotNull(regionsNode, storeName + ": regions should be created");
        assertTrue(regionsNode.isArray());
        assertEquals(2, regionsNode.size());
        assertEquals("US", regionsNode.get(0).asText());
        assertEquals("EU", regionsNode.get(1).asText());

        // Verify other fields preserved
        assertEquals("TestBrand", resultJson.get("props").get("brand").asText());
        assertEquals(200, resultJson.get("sales").get("total").asInt());
      }

      @ParameterizedTest(name = "{0}: APPEND_TO_LIST on non-array field (TEXT column)")
      @ArgumentsSource(AllStoresProvider.class)
      void testAppendToListOnNonArrayField(String storeName) throws Exception {
        String docId = generateDocId("append-non-array");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // Try to APPEND_TO_LIST to 'item' which is a TEXT field
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("item")
                    .operator(UpdateOperator.APPEND_TO_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"value1", "value2"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        assertThrows(Exception.class, () -> collection.update(query, updates, options));
      }

      @ParameterizedTest(name = "{0}: APPEND_TO_LIST on non-array field (INTEGER column)")
      @ArgumentsSource(AllStoresProvider.class)
      void testAppendToListOnIntegerField(String storeName) throws Exception {
        String docId = generateDocId("append-integer");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // Try to APPEND_TO_LIST to 'price' which is an INTEGER field
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("price")
                    .operator(UpdateOperator.APPEND_TO_LIST)
                    .subDocumentValue(SubDocumentValue.of(new Integer[] {100, 200}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        assertThrows(Exception.class, () -> collection.update(query, updates, options));
      }
    }

    @Nested
    @DisplayName("ADD_TO_LIST_IF_ABSENT Operator Tests")
    class AddToListIfAbsentOperatorTests {

      @ParameterizedTest(name = "{0}: ADD_TO_LIST_IF_ABSENT for top-level and nested arrays")
      @ArgumentsSource(AllStoresProvider.class)
      void testAddToListIfAbsentAllCases(String storeName) throws Exception {
        String docId = generateDocId("addifabsent");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        List<SubDocumentUpdate> updates =
            List.of(
                // Top-level: 'tag1' exists, 'newTag' is new → adds only 'newTag'
                SubDocumentUpdate.builder()
                    .subDocument("tags")
                    .operator(UpdateOperator.ADD_TO_LIST_IF_ABSENT)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"tag1", "newTag"}))
                    .build(),
                // Nested JSONB: 'red' exists, 'green' is new → adds only 'green'
                SubDocumentUpdate.builder()
                    .subDocument("props.colors")
                    .operator(UpdateOperator.ADD_TO_LIST_IF_ABSENT)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"red", "green"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        // Verify top-level: original 2 + 1 new unique = 3 (order not guaranteed)
        JsonNode tagsNode = resultJson.get("tags");
        assertTrue(tagsNode.isArray());
        assertEquals(3, tagsNode.size(), storeName + ": only newTag added, tag1 already exists");
        Set<String> tagValues = new HashSet<>();
        tagsNode.forEach(n -> tagValues.add(n.asText()));
        assertTrue(tagValues.contains("tag1"));
        assertTrue(tagValues.contains("tag2"));
        assertTrue(tagValues.contains("newTag"));

        // Verify nested JSONB: original 2 + 1 new unique = 3 (order not guaranteed)
        JsonNode colorsNode = resultJson.get("props").get("colors");
        assertTrue(colorsNode.isArray());
        assertEquals(3, colorsNode.size(), storeName + ": only green added, red already exists");
        Set<String> colorValues = new HashSet<>();
        colorsNode.forEach(n -> colorValues.add(n.asText()));
        assertTrue(colorValues.contains("red"));
        assertTrue(colorValues.contains("blue"));
        assertTrue(colorValues.contains("green"));
      }

      @ParameterizedTest(name = "{0}: ADD_TO_LIST_IF_ABSENT on non-array field (TEXT column)")
      @ArgumentsSource(AllStoresProvider.class)
      void testAddToListIfAbsentOnNonArrayField(String storeName) throws Exception {
        String docId = generateDocId("addifabsent-non-array");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // Try to ADD_TO_LIST_IF_ABSENT to 'item' which is a TEXT field
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("item")
                    .operator(UpdateOperator.ADD_TO_LIST_IF_ABSENT)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"value1", "value2"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        assertThrows(Exception.class, () -> collection.update(query, updates, options));
      }
    }

    @Nested
    @DisplayName("REMOVE_ALL_FROM_LIST Operator Tests")
    class RemoveAllFromListOperatorTests {

      @ParameterizedTest(name = "{0}: REMOVE_ALL_FROM_LIST for top-level and nested arrays")
      @ArgumentsSource(AllStoresProvider.class)
      void testRemoveAllFromListAllCases(String storeName) throws Exception {
        String docId = generateDocId("remove");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        List<SubDocumentUpdate> updates =
            List.of(
                // Top-level: remove 'tag1' → leaves tag2
                SubDocumentUpdate.builder()
                    .subDocument("tags")
                    .operator(UpdateOperator.REMOVE_ALL_FROM_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"tag1"}))
                    .build(),
                // Nested JSONB: remove 'red' → leaves blue
                SubDocumentUpdate.builder()
                    .subDocument("props.colors")
                    .operator(UpdateOperator.REMOVE_ALL_FROM_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"red"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = collection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());

        // Verify top-level: tag1 removed, tag2 remains
        JsonNode tagsNode = resultJson.get("tags");
        assertTrue(tagsNode.isArray());
        assertEquals(1, tagsNode.size(), storeName + ": tag1 removed, tag2 remains");
        assertEquals("tag2", tagsNode.get(0).asText());

        // Verify nested JSONB: red removed, blue remains
        JsonNode colorsNode = resultJson.get("props").get("colors");
        assertTrue(colorsNode.isArray());
        assertEquals(1, colorsNode.size(), storeName + ": red removed, blue remains");
        assertEquals("blue", colorsNode.get(0).asText());

        // Verify numbers unchanged (no-op since we didn't update it)
        JsonNode numbersNode = resultJson.get("numbers");
        assertTrue(numbersNode.isArray());
        assertEquals(3, numbersNode.size());
      }

      @ParameterizedTest(name = "{0}: REMOVE_ALL_FROM_LIST on non-array field (TEXT column)")
      @ArgumentsSource(AllStoresProvider.class)
      void testRemoveAllFromListOnNonArrayField(String storeName) throws Exception {
        String docId = generateDocId("remove-non-array");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // Try to REMOVE_ALL_FROM_LIST from 'item' which is a TEXT field
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("item")
                    .operator(UpdateOperator.REMOVE_ALL_FROM_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"value1"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        assertThrows(Exception.class, () -> collection.update(query, updates, options));
      }
    }

    @Nested
    class AllOperatorTests {

      @ParameterizedTest
      @ArgumentsSource(AllStoresProvider.class)
      void testMultipleUpdatesOnSameFieldThrowsException(String storeName) throws IOException {
        String docId = generateDocId("multiple-updates-on-same-field");
        insertTestDocument(docId);

        Collection collection = getCollection(storeName);
        Query query = buildQueryById(docId);

        // top-level primitives
        List<SubDocumentUpdate> topLevelPrimitiveUpdates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("price")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(5))
                    .build(),
                SubDocumentUpdate.builder()
                    .subDocument("price")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(-15))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        // Since there are multiple updates on the same field, it should throw an exception
        assertThrows(
            Exception.class, () -> collection.update(query, topLevelPrimitiveUpdates, options));

        // top-level arrays
        List<SubDocumentUpdate> topLevelArrayUpdates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("tags")
                    .operator(UpdateOperator.APPEND_TO_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"tag1", "tag2"}))
                    .build(),
                SubDocumentUpdate.builder()
                    .subDocument("tags")
                    .operator(UpdateOperator.REMOVE_ALL_FROM_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"tag2"}))
                    .build());

        assertThrows(
            Exception.class, () -> collection.update(query, topLevelArrayUpdates, options));

        // nested array updates
        List<SubDocumentUpdate> nestedArrayUpdates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("sales.regions")
                    .operator(UpdateOperator.SET)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"US", "EU", "APAC"}))
                    .build(),
                SubDocumentUpdate.builder()
                    .subDocument("sales.regions")
                    .operator(UpdateOperator.ADD_TO_LIST_IF_ABSENT)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"EMEA"}))
                    .build());

        assertThrows(Exception.class, () -> collection.update(query, nestedArrayUpdates, options));

        // nested primitives
        List<SubDocumentUpdate> nestedPrimitiveUpdates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("props.brand")
                    .operator(UpdateOperator.SET)
                    .subDocumentValue(SubDocumentValue.of("NewBrand"))
                    .build(),
                SubDocumentUpdate.builder()
                    .subDocument("props.brand")
                    .operator(UpdateOperator.ADD_TO_LIST_IF_ABSENT)
                    .subDocumentValue(SubDocumentValue.of("NewBrand2"))
                    .build());

        assertThrows(
            Exception.class, () -> collection.update(query, nestedPrimitiveUpdates, options));
      }
    }
  }
}
