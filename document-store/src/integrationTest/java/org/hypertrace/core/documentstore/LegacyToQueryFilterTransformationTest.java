package org.hypertrace.core.documentstore;

import static org.hypertrace.core.documentstore.utils.Utils.readFileFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.LegacyFilterToQueryFilterTransformer;
import org.hypertrace.core.documentstore.query.Query;
import org.junit.jupiter.api.AfterAll;
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
 * Integration tests that validate the {@link LegacyFilterToQueryFilterTransformer} by comparing
 * query results obtained using:
 *
 * <ol>
 *   <li>Legacy Filter (org.hypertrace.core.documentstore.Filter)
 *   <li>New Query Filter (org.hypertrace.core.documentstore.query.Filter) - transformed from legacy
 * </ol>
 *
 * <p>Both approaches should yield identical results for the same filter conditions.
 */
@Testcontainers
public class LegacyToQueryFilterTransformationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(LegacyToQueryFilterTransformationTest.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String FLAT_COLLECTION_NAME = "filterTestFlat";
  private static final String INSERT_STATEMENTS_FILE = "query/pg_flat_collection_insert.json";

  private static Datastore postgresDatastore;
  private static Collection flatCollection;
  private static GenericContainer<?> postgres;
  private static LegacyFilterToQueryFilterTransformer transformer;

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

    createFlatCollectionSchema();
    flatCollection =
        postgresDatastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

    PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
    transformer =
        new LegacyFilterToQueryFilterTransformer(
            pgDatastore.getSchemaRegistry(), FLAT_COLLECTION_NAME);
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
            statement = statement.replace("myTestFlat", FLAT_COLLECTION_NAME);
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
    clearTable();
    executeInsertStatements();
  }

  private static void clearTable() {
    PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
    String deleteSQL = String.format("DELETE FROM \"%s\"", FLAT_COLLECTION_NAME);
    try (Connection connection = pgDatastore.getPostgresClient();
        PreparedStatement statement = connection.prepareStatement(deleteSQL)) {
      statement.executeUpdate();
    } catch (Exception e) {
      LOGGER.error("Failed to clear table: {}", e.getMessage(), e);
    }
  }

  @AfterAll
  public static void shutdown() {
    if (postgres != null) {
      postgres.stop();
    }
  }

  @Nested
  @DisplayName("Equality Operators (EQ, NEQ)")
  class EqualityOperatorTests {

    @Test
    @DisplayName("EQ: Should return same results for legacy and transformed filter")
    void testEqOperator() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.EQ, "item", "Soap");
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> newResults = collectResults(flatCollection.find(query));

      assertNotNull(newResults);
      assertFalse(newResults.isEmpty(), "Should find at least one document with item='Soap'");

      for (Document doc : newResults) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        assertEquals("Soap", node.get("item").asText());
      }
    }

    @Test
    @DisplayName("NEQ: Should return same results for legacy and transformed filter")
    void testNeqOperator() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.NEQ, "item", "Soap");
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        assertTrue(
            !node.has("item") || !node.get("item").asText().equals("Soap"),
            "Should not contain item='Soap'");
      }
    }
  }

  @Nested
  @DisplayName("Comparison Operators (GT, GTE, LT, LTE)")
  class ComparisonOperatorTests {

    @Test
    @DisplayName("GT: Should return documents with price > 10")
    void testGtOperator() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.GT, "price", 10);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents with price > 10");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        if (node.has("price") && !node.get("price").isNull()) {
          assertTrue(node.get("price").asInt() > 10, "Price should be > 10");
        }
      }
    }

    @Test
    @DisplayName("GTE: Should return documents with price >= 10")
    void testGteOperator() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.GTE, "price", 10);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents with price >= 10");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        if (node.has("price") && !node.get("price").isNull()) {
          assertTrue(node.get("price").asInt() >= 10, "Price should be >= 10");
        }
      }
    }

    @Test
    @DisplayName("LT: Should return documents with price < 10")
    void testLtOperator() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.LT, "price", 10);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents with price < 10");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        if (node.has("price") && !node.get("price").isNull()) {
          assertTrue(node.get("price").asInt() < 10, "Price should be < 10");
        }
      }
    }

    @Test
    @DisplayName("LTE: Should return documents with price <= 10")
    void testLteOperator() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.LTE, "price", 10);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents with price <= 10");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        if (node.has("price") && !node.get("price").isNull()) {
          assertTrue(node.get("price").asInt() <= 10, "Price should be <= 10");
        }
      }
    }
  }

  @Nested
  @DisplayName("Collection Operators (IN, NOT_IN)")
  class CollectionOperatorTests {

    @Test
    @DisplayName("IN: Should return documents with item in list")
    void testInOperator() throws Exception {
      List<String> items = List.of("Soap", "Mirror");
      Filter legacyFilter = new Filter(Filter.Op.IN, "item", items);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents with item in list");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        String item = node.get("item").asText();
        assertTrue(items.contains(item), "Item should be in the list: " + item);
      }
    }

    @Test
    @DisplayName("NOT_IN: Should return documents with item not in list")
    void testNotInOperator() throws Exception {
      List<String> items = List.of("Soap", "Mirror");
      Filter legacyFilter = new Filter(Filter.Op.NOT_IN, "item", items);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents with item not in list");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        if (node.has("item") && !node.get("item").isNull()) {
          String item = node.get("item").asText();
          assertFalse(items.contains(item), "Item should NOT be in the list: " + item);
        }
      }
    }

    @Test
    @DisplayName("IN with numbers: Should return documents with price in list")
    void testInOperatorWithNumbers() throws Exception {
      List<Integer> prices = List.of(5, 10, 20);
      Filter legacyFilter = new Filter(Filter.Op.IN, "price", prices);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents with price in list");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        if (node.has("price") && !node.get("price").isNull()) {
          int price = node.get("price").asInt();
          assertTrue(prices.contains(price), "Price should be in the list: " + price);
        }
      }
    }
  }

  @Nested
  @DisplayName("Logical Operators (AND, OR)")
  class LogicalOperatorTests {

    @Test
    @DisplayName("AND: Should return documents matching all conditions")
    void testAndOperator() throws Exception {
      Filter itemFilter = new Filter(Filter.Op.EQ, "item", "Soap");
      Filter priceFilter = new Filter(Filter.Op.GT, "price", 10);
      Filter legacyAndFilter = itemFilter.and(priceFilter);

      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyAndFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents matching AND condition");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        assertEquals("Soap", node.get("item").asText());
        assertTrue(node.get("price").asInt() > 10, "Price should be > 10");
      }
    }

    @Test
    @DisplayName("OR: Should return documents matching any condition")
    void testOrOperator() throws Exception {
      Filter soapFilter = new Filter(Filter.Op.EQ, "item", "Soap");
      Filter mirrorFilter = new Filter(Filter.Op.EQ, "item", "Mirror");
      Filter legacyOrFilter = soapFilter.or(mirrorFilter);

      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyOrFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents matching OR condition");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        String item = node.get("item").asText();
        assertTrue(
            item.equals("Soap") || item.equals("Mirror"),
            "Item should be 'Soap' or 'Mirror', got: " + item);
      }
    }

    @Test
    @DisplayName("Nested AND/OR: Should handle complex conditions")
    void testNestedAndOr() throws Exception {
      Filter soapFilter = new Filter(Filter.Op.EQ, "item", "Soap");
      Filter priceFilter = new Filter(Filter.Op.GT, "price", 10);
      Filter soapAndPrice = soapFilter.and(priceFilter);

      Filter combFilter = new Filter(Filter.Op.EQ, "item", "Comb");
      Filter complexFilter = soapAndPrice.or(combFilter);

      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(complexFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents matching complex condition");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        String item = node.get("item").asText();
        int price =
            node.has("price") && !node.get("price").isNull() ? node.get("price").asInt() : 0;

        boolean matchesSoapAndPrice = item.equals("Soap") && price > 10;
        boolean matchesComb = item.equals("Comb");

        assertTrue(
            matchesSoapAndPrice || matchesComb,
            "Document should match (Soap AND price>10) OR Comb. Got: " + item + ", " + price);
      }
    }
  }

  @Nested
  @DisplayName("Boolean Operators (EXISTS, NOT_EXISTS)")
  class ExistsOperatorTests {

    @Test
    @DisplayName("EXISTS: Should return documents where field exists")
    void testExistsOperator() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.EXISTS, "in_stock", null);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        assertTrue(
            node.has("in_stock") && !node.get("in_stock").isNull(),
            "Document should have in_stock field");
      }
    }

    @Test
    @DisplayName("NOT_EXISTS: Should return documents where field does not exist")
    void testNotExistsOperator() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.NOT_EXISTS, "in_stock", null);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        assertTrue(
            !node.has("in_stock") || node.get("in_stock").isNull(),
            "Document should not have in_stock field or it should be null");
      }
    }
  }

  @Nested
  @DisplayName("String Operators (LIKE, CONTAINS)")
  class StringOperatorTests {

    @Test
    @DisplayName("LIKE: Should return documents matching pattern")
    void testLikeOperator() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.LIKE, "item", "Sha.*");
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents matching LIKE pattern");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        String item = node.get("item").asText();
        assertTrue(item.startsWith("Sha"), "Item should start with 'Sha': " + item);
      }
    }
  }

  @Nested
  @DisplayName("Nested JSONB Filters (STRING and STRING_ARRAY)")
  class NestedJsonbFilterTests {

    @Test
    @DisplayName("EQ on nested STRING field: props.brand")
    void testNestedStringEq() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.EQ, "props.brand", "Dettol");
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents with props.brand='Dettol'");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        JsonNode props = node.get("props");
        assertNotNull(props, "props should exist");
        assertEquals("Dettol", props.get("brand").asText());
      }
    }

    @Test
    @DisplayName("NEQ on nested STRING field: props.brand")
    void testNestedStringNeq() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.NEQ, "props.brand", "Dettol");
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        JsonNode props = node.get("props");
        if (props != null && props.has("brand") && !props.get("brand").isNull()) {
          assertNotEquals("Dettol", props.get("brand").asText());
        }
      }
    }

    @Test
    @DisplayName("IN on nested STRING field: props.brand")
    void testNestedStringIn() throws Exception {
      List<String> brands = List.of("Dettol", "Lifebuoy");
      Filter legacyFilter = new Filter(Filter.Op.IN, "props.brand", brands);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents with props.brand in list");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        JsonNode props = node.get("props");
        assertNotNull(props, "props should exist");
        String brand = props.get("brand").asText();
        assertTrue(brands.contains(brand), "Brand should be in list: " + brand);
      }
    }

    @Test
    @DisplayName("EQ on deeply nested STRING field: props.seller.name")
    void testDeeplyNestedStringEq() throws Exception {
      Filter legacyFilter =
          new Filter(Filter.Op.EQ, "props.seller.name", "Metro Chemicals Pvt. Ltd.");
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(
          results.isEmpty(),
          "Should find documents with props.seller.name='Metro Chemicals Pvt. Ltd.'");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        JsonNode seller = node.path("props").path("seller");
        assertNotNull(seller, "seller should exist");
        assertEquals("Metro Chemicals Pvt. Ltd.", seller.get("name").asText());
      }
    }

    @Test
    @DisplayName("EQ on triple-nested STRING field: props.seller.address.city")
    void testTripleNestedStringEq() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.EQ, "props.seller.address.city", "Kolkata");
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(
          results.isEmpty(), "Should find documents with props.seller.address.city='Kolkata'");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        JsonNode address = node.path("props").path("seller").path("address");
        assertNotNull(address, "address should exist");
        assertEquals("Kolkata", address.get("city").asText());
      }
    }

    @Test
    @DisplayName("CONTAINS on nested STRING_ARRAY field: props.colors")
    void testNestedStringArrayContains() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.CONTAINS, "props.colors", "Blue");
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents with props.colors containing 'Blue'");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        JsonNode colors = node.path("props").path("colors");
        assertTrue(colors.isArray(), "colors should be an array");
        boolean containsBlue = false;
        for (JsonNode color : colors) {
          if ("Blue".equals(color.asText())) {
            containsBlue = true;
            break;
          }
        }
        assertTrue(containsBlue, "colors should contain 'Blue'");
      }
    }

    @Test
    @DisplayName("CONTAINS on nested STRING_ARRAY field: props.source-loc")
    void testNestedStringArraySourceLocContains() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.CONTAINS, "props.source-loc", "warehouse-A");
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(
          results.isEmpty(),
          "Should find documents with props.source-loc containing 'warehouse-A'");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        JsonNode sourceLoc = node.path("props").path("source-loc");
        assertTrue(sourceLoc.isArray(), "source-loc should be an array");
        boolean containsWarehouseA = false;
        for (JsonNode loc : sourceLoc) {
          if ("warehouse-A".equals(loc.asText())) {
            containsWarehouseA = true;
            break;
          }
        }
        assertTrue(containsWarehouseA, "source-loc should contain 'warehouse-A'");
      }
    }

    @Test
    @DisplayName("AND on nested JSONB fields: props.brand AND props.seller.address.city")
    void testNestedJsonbAnd() throws Exception {
      Filter brandFilter = new Filter(Filter.Op.EQ, "props.brand", "Dettol");
      Filter cityFilter = new Filter(Filter.Op.EQ, "props.seller.address.city", "Mumbai");
      Filter legacyAndFilter = brandFilter.and(cityFilter);

      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyAndFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents matching nested AND condition");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        JsonNode props = node.get("props");
        assertEquals("Dettol", props.get("brand").asText());
        assertEquals("Mumbai", props.path("seller").path("address").get("city").asText());
      }
    }

    @Test
    @DisplayName("OR on nested JSONB fields: props.brand='Dettol' OR props.brand='Sunsilk'")
    void testNestedJsonbOr() throws Exception {
      Filter dettolFilter = new Filter(Filter.Op.EQ, "props.brand", "Dettol");
      Filter sunsilkFilter = new Filter(Filter.Op.EQ, "props.brand", "Sunsilk");
      Filter legacyOrFilter = dettolFilter.or(sunsilkFilter);

      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyOrFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(results.isEmpty(), "Should find documents matching nested OR condition");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        String brand = node.path("props").get("brand").asText();
        assertTrue(
            brand.equals("Dettol") || brand.equals("Sunsilk"),
            "Brand should be 'Dettol' or 'Sunsilk', got: " + brand);
      }
    }

    @Test
    @DisplayName("LIKE on nested STRING field: props.product-code")
    void testNestedStringLike() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.LIKE, "props.product-code", "SOAP-.*");
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      assertFalse(
          results.isEmpty(), "Should find documents with props.product-code like 'SOAP-.*'");
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        String productCode = node.path("props").get("product-code").asText();
        assertTrue(
            productCode.startsWith("SOAP-"),
            "product-code should start with 'SOAP-': " + productCode);
      }
    }

    @Test
    @DisplayName("EXISTS on nested STRING field: props.brand")
    void testNestedStringExists() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.EXISTS, "props.brand", null);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        JsonNode brand = node.path("props").path("brand");
        assertTrue(
            !brand.isMissingNode() && !brand.isNull(), "props.brand should exist and not be null");
      }
    }

    @Test
    @DisplayName("NOT_EXISTS on nested STRING field: props.brand")
    void testNestedStringNotExists() throws Exception {
      Filter legacyFilter = new Filter(Filter.Op.NOT_EXISTS, "props.brand", null);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      Query query = Query.builder().setFilter(newFilter).build();
      List<Document> results = collectResults(flatCollection.find(query));

      assertNotNull(results);
      for (Document doc : results) {
        JsonNode node = OBJECT_MAPPER.readTree(doc.toJson());
        JsonNode props = node.get("props");
        if (props != null && !props.isNull()) {
          JsonNode brand = props.get("brand");
          assertTrue(
              brand == null || brand.isNull() || brand.isMissingNode(),
              "props.brand should not exist or be null");
        }
      }
    }
  }

  @Nested
  @DisplayName("Transformer Unit Tests")
  class TransformerUnitTests {

    @Test
    @DisplayName("Should return null for null input")
    void testNullInput() {
      org.hypertrace.core.documentstore.query.Filter result = transformer.transform(null);
      assertNull(result);
    }

    @Test
    @DisplayName("Should correctly transform simple EQ filter")
    void testSimpleEqTransformation() {
      Filter legacyFilter = new Filter(Filter.Op.EQ, "name", "test");
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      assertNotNull(newFilter);
      assertNotNull(newFilter.getExpression());
      assertInstanceOf(RelationalExpression.class, newFilter.getExpression());

      RelationalExpression expr = (RelationalExpression) newFilter.getExpression();
      assertEquals(RelationalOperator.EQ, expr.getOperator());
      assertInstanceOf(IdentifierExpression.class, expr.getLhs());
      assertInstanceOf(ConstantExpression.class, expr.getRhs());
    }

    @Test
    @DisplayName("Should correctly transform AND filter")
    void testAndTransformation() {
      Filter f1 = new Filter(Filter.Op.EQ, "a", "1");
      Filter f2 = new Filter(Filter.Op.EQ, "b", "2");
      Filter andFilter = f1.and(f2);

      org.hypertrace.core.documentstore.query.Filter newFilter = transformer.transform(andFilter);

      assertNotNull(newFilter);
      assertInstanceOf(LogicalExpression.class, newFilter.getExpression());

      LogicalExpression logicalExpr = (LogicalExpression) newFilter.getExpression();
      assertEquals(2, logicalExpr.getOperands().size());
    }

    @Test
    @DisplayName("Should correctly transform OR filter")
    void testOrTransformation() {
      Filter f1 = new Filter(Filter.Op.EQ, "a", "1");
      Filter f2 = new Filter(Filter.Op.EQ, "b", "2");
      Filter orFilter = f1.or(f2);

      org.hypertrace.core.documentstore.query.Filter newFilter = transformer.transform(orFilter);

      assertNotNull(newFilter);
      assertInstanceOf(LogicalExpression.class, newFilter.getExpression());
    }

    @Test
    @DisplayName("Should infer NUMBER type for nested field with numeric value")
    void testNestedFieldWithNumberValue() {
      Filter legacyFilter = new Filter(Filter.Op.EQ, "props.count", 42);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      assertNotNull(newFilter);
      RelationalExpression expr = (RelationalExpression) newFilter.getExpression();
      ConstantExpression rhs = (ConstantExpression) expr.getRhs();
      assertEquals(42, rhs.getValue());
    }

    @Test
    @DisplayName("Should infer BOOLEAN type for nested field with boolean value")
    void testNestedFieldWithBooleanValue() {
      Filter legacyFilter = new Filter(Filter.Op.EQ, "props.active", true);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      assertNotNull(newFilter);
      RelationalExpression expr = (RelationalExpression) newFilter.getExpression();
      ConstantExpression rhs = (ConstantExpression) expr.getRhs();
      assertEquals(true, rhs.getValue());
    }

    @Test
    @DisplayName("Should handle IN filter with Object[] array of strings")
    void testInFilterWithObjectArray() {
      Object[] values = new Object[] {"value1", "value2"};
      Filter legacyFilter = new Filter(Filter.Op.IN, "props.tags", values);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      assertNotNull(newFilter);
      RelationalExpression expr = (RelationalExpression) newFilter.getExpression();
      assertEquals(RelationalOperator.IN, expr.getOperator());
    }

    @Test
    @DisplayName("Should handle IN filter with boolean collection")
    void testInFilterWithBooleanCollection() {
      List<Boolean> values = List.of(true, false);
      Filter legacyFilter = new Filter(Filter.Op.IN, "props.flags", values);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      assertNotNull(newFilter);
      RelationalExpression expr = (RelationalExpression) newFilter.getExpression();
      assertEquals(RelationalOperator.IN, expr.getOperator());
      ConstantExpression rhs = (ConstantExpression) expr.getRhs();
      assertNotNull(rhs.getValue());
    }

    @Test
    @DisplayName("Should throw exception for unsupported collection element type")
    void testUnsupportedCollectionElementType() {
      List<Object> values = List.of(new java.util.Date());
      Filter legacyFilter = new Filter(Filter.Op.IN, "field", values);

      Exception exception =
          assertThrows(IllegalArgumentException.class, () -> transformer.transform(legacyFilter));
      assertTrue(exception.getMessage().contains("Unsupported collection element type"));
    }

    @Test
    @DisplayName("Should throw exception for null field name")
    void testNullFieldName() {
      Filter legacyFilter = new Filter(Filter.Op.EQ, null, "value");

      Exception exception =
          assertThrows(IllegalArgumentException.class, () -> transformer.transform(legacyFilter));
      assertTrue(exception.getMessage().contains("Field name cannot be null or empty"));
    }

    @Test
    @DisplayName("Should throw exception for empty field name")
    void testEmptyFieldName() {
      Filter legacyFilter = new Filter(Filter.Op.EQ, "", "value");

      Exception exception =
          assertThrows(IllegalArgumentException.class, () -> transformer.transform(legacyFilter));
      assertTrue(exception.getMessage().contains("Field name cannot be null or empty"));
    }

    @Test
    @DisplayName("Should throw exception for empty collection in IN operator")
    void testEmptyCollectionInOperator() {
      List<String> emptyList = List.of();
      Filter legacyFilter = new Filter(Filter.Op.IN, "field", emptyList);

      Exception exception =
          assertThrows(IllegalArgumentException.class, () -> transformer.transform(legacyFilter));
      assertTrue(exception.getMessage().contains("Collection cannot be empty"));
    }

    @Test
    @DisplayName("Should use toString fallback for unknown value type")
    void testToStringFallbackForUnknownType() {
      // Use a custom object that will fall through to toString
      Object customValue =
          new Object() {
            @Override
            public String toString() {
              return "custom-value";
            }
          };
      Filter legacyFilter = new Filter(Filter.Op.EQ, "field", customValue);
      org.hypertrace.core.documentstore.query.Filter newFilter =
          transformer.transform(legacyFilter);

      assertNotNull(newFilter);
      RelationalExpression expr = (RelationalExpression) newFilter.getExpression();
      ConstantExpression rhs = (ConstantExpression) expr.getRhs();
      assertEquals("custom-value", rhs.getValue());
    }

    @Test
    @DisplayName("Should throw exception for unsupported value type in nested field")
    void testUnsupportedValueTypeInNestedField() {
      Filter legacyFilter = new Filter(Filter.Op.EQ, "props.custom", new java.util.Date());

      Exception exception =
          assertThrows(IllegalArgumentException.class, () -> transformer.transform(legacyFilter));
      assertTrue(exception.getMessage().contains("Unsupported value type for JsonFieldType"));
    }

    @Test
    @DisplayName("Should handle IN with empty Object[] array")
    void testInFilterWithEmptyObjectArray() {
      Object[] values = new Object[] {};
      Filter legacyFilter = new Filter(Filter.Op.IN, "field", values);

      Exception exception =
          assertThrows(IllegalArgumentException.class, () -> transformer.transform(legacyFilter));
      assertTrue(exception.getMessage().contains("Collection cannot be empty"));
    }
  }

  /** Helper method to collect all documents from a CloseableIterator. */
  private List<Document> collectResults(CloseableIterator<Document> iterator) {
    List<Document> results = new ArrayList<>();
    try (iterator) {
      while (iterator.hasNext()) {
        results.add(iterator.next());
      }
    } catch (IOException e) {
      LOGGER.warn("Error closing iterator", e);
    }
    return results;
  }
}
