package org.hypertrace.core.documentstore.mongo;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.FIRST;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.MULTIPLY;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.OR;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;
import static org.hypertrace.core.documentstore.expression.operators.SortingOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortingOrder.DESC;
import static org.hypertrace.core.documentstore.utils.Utils.convertDocumentToMap;
import static org.hypertrace.core.documentstore.utils.Utils.convertJsonToMap;
import static org.hypertrace.core.documentstore.utils.Utils.createDocumentsFromResource;
import static org.hypertrace.core.documentstore.utils.Utils.readFileFromResource;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.Sort;
import org.hypertrace.core.documentstore.query.SortingSpec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class MongoQueryExecutorIntegrationTest {
  private static final String COLLECTION_NAME = "mongoQueryExecutorTest";

  private static GenericContainer<?> mongo;
  private static Collection collection;

  @BeforeAll
  public static void init() throws IOException {
    mongo =
        new GenericContainer<>(DockerImageName.parse("mongo:4.4.0"))
            .withExposedPorts(27017)
            .waitingFor(Wait.forListeningPort());
    mongo.start();

    DatastoreProvider.register("MONGO", MongoDatastore.class);

    Map<String, String> mongoConfig = new HashMap<>();
    mongoConfig.putIfAbsent("host", "localhost");
    mongoConfig.putIfAbsent("port", mongo.getMappedPort(27017).toString());
    Config config = ConfigFactory.parseMap(mongoConfig);

    Datastore datastore = DatastoreProvider.getDatastore("Mongo", config);

    datastore.deleteCollection(COLLECTION_NAME);
    datastore.createCollection(COLLECTION_NAME, null);
    collection = datastore.getCollection(COLLECTION_NAME);

    Map<Key, Document> documents = createDocumentsFromResource("mongo/collection_data.json");
    collection.bulkUpsert(documents);
  }

  @AfterAll
  public static void shutdown() {
    mongo.stop();
  }

  @Test
  public void testFindAll() throws IOException {
    Query query = Query.builder().build();

    Iterator<Document> resultDocs = collection.find(query);
    assertSizeEqual(resultDocs, "mongo/collection_data.json");
  }

  @Test
  public void testFindSimple() throws IOException {
    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("price")),
            SelectionSpec.of(IdentifierExpression.of("quantity")),
            SelectionSpec.of(IdentifierExpression.of("date")));
    Selection selection = Selection.builder().selectionSpecs(selectionSpecs).build();
    Filter filter =
        Filter.builder()
            .expression(
                RelationalExpression.of(
                    IdentifierExpression.of("item"),
                    NOT_IN,
                    ConstantExpression.ofStrings(List.of("Soap", "Bottle"))))
            .build();

    Query query = Query.builder().setSelection(selection).setFilter(filter).build();

    Iterator<Document> resultDocs = collection.find(query);
    assertDocsEqual(resultDocs, "mongo/simple_filter_response.json");
  }

  @Test
  public void testFindWithSortingAndPagination() throws IOException {
    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("price")),
            SelectionSpec.of(IdentifierExpression.of("quantity")),
            SelectionSpec.of(IdentifierExpression.of("date")));
    Selection selection = Selection.builder().selectionSpecs(selectionSpecs).build();

    Filter filter =
        Filter.builder()
            .expression(
                RelationalExpression.of(
                    IdentifierExpression.of("item"),
                    IN,
                    ConstantExpression.ofStrings(List.of("Mirror", "Comb", "Shampoo", "Bottle"))))
            .build();

    Sort sort =
        Sort.builder()
            .sortingSpec(SortingSpec.of(IdentifierExpression.of("quantity"), DESC))
            .sortingSpec(SortingSpec.of(IdentifierExpression.of("item"), ASC))
            .build();

    Pagination pagination = Pagination.builder().offset(1).limit(3).build();

    Query query =
        Query.builder()
            .setSelection(selection)
            .setFilter(filter)
            .setSort(sort)
            .setPagination(pagination)
            .build();

    Iterator<Document> resultDocs = collection.find(query);
    assertDocsEqual(resultDocs, "mongo/filter_with_sorting_and_pagination_response.json");
  }

  @Test
  public void testFindWithNestedFields() throws IOException {
    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("price")),
            SelectionSpec.of(IdentifierExpression.of("props.seller.name"), "seller"),
            SelectionSpec.of(IdentifierExpression.of("props.brand")),
            SelectionSpec.of(IdentifierExpression.of("props.seller.address.city")));

    Query query =
        Query.builder()
            .addSelections(selectionSpecs)
            .setFilter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("item"),
                            IN,
                            ConstantExpression.ofStrings(List.of("Mirror", "Comb", "Shampoo"))))
                    .operator(OR)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("props.seller.address.pincode"),
                            EQ,
                            ConstantExpression.of(700007)))
                    .build())
            .addSort(IdentifierExpression.of("props.brand"), ASC)
            .addSort(IdentifierExpression.of("item"), ASC)
            .addSort(IdentifierExpression.of("props.seller.address.city"), ASC)
            .build();

    Iterator<Document> resultDocs = collection.find(query);
    assertDocsEqual(resultDocs, "mongo/filter_on_nested_fields_response.json");
  }

  @Test
  public void testAggregateEmpty() throws IOException {
    Query query = Query.builder().build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertSizeEqual(resultDocs, "mongo/collection_data.json");
  }

  @Test
  public void testAggregateSimple() throws IOException {
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(SUM, ConstantExpression.of(1)), "count")
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsEqual(resultDocs, "mongo/count_response.json");
  }

  @Test
  public void testAggregateWithFiltersAndOrdering() throws IOException {
    Query query =
        Query.builder()
            .addSelection(
                AggregateExpression.of(
                    SUM,
                    FunctionExpression.builder()
                        .operand(IdentifierExpression.of("price"))
                        .operator(MULTIPLY)
                        .operand(IdentifierExpression.of("quantity"))
                        .build()),
                "total")
            .addSelection(AggregateExpression.of(FIRST, IdentifierExpression.of("item")), "item")
            .addAggregation(IdentifierExpression.of("item"))
            .addSort(IdentifierExpression.of("total"), DESC)
            .setAggregationFilter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("total"), GTE, ConstantExpression.of(11)))
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("total"), LTE, ConstantExpression.of(99)))
                    .build())
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("quantity"), NEQ, ConstantExpression.of(10)))
            .setLimit(10)
            .setOffset(0)
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsEqual(resultDocs, "mongo/sum_response.json");
  }

  @Test
  public void testAggregateWithNestedFields() throws IOException {
    Query query =
        Query.builder()
            .addSelection(
                AggregateExpression.of(FIRST, IdentifierExpression.of("item")), "first_item")
            .addSelection(
                AggregateExpression.of(
                    FIRST, IdentifierExpression.of("props.seller.address.pincode")),
                "pincode")
            .addSelection(AggregateExpression.of(SUM, ConstantExpression.of(1)), "num_items")
            .addAggregation(IdentifierExpression.of("props.seller.address.pincode"))
            .addSort(IdentifierExpression.of("pincode"), DESC)
            .addSort(IdentifierExpression.of("first_item"), ASC)
            .setAggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("num_items"), GT, ConstantExpression.of(1)))
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsEqual(resultDocs, "mongo/aggregate_on_nested_fields_response.json");
  }

  @Test
  public void testDistinctCount() throws IOException {
    Query query =
        Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("item"))
            .setAggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(1000)))
            .addSort(IdentifierExpression.of("qty_count"), DESC)
            .addSort(IdentifierExpression.of("item"), DESC)
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsEqual(resultDocs, "mongo/distinct_count_response.json");
  }

  private static void assertDocsEqual(Iterator<Document> documents, String filePath)
      throws IOException {
    String fileContent = readFileFromResource(filePath).orElseThrow();
    List<Map<String, Object>> expected = convertJsonToMap(fileContent);

    List<Map<String, Object>> actual = new ArrayList<>();
    while (documents.hasNext()) {
      actual.add(convertDocumentToMap(documents.next()));
    }

    Assertions.assertEquals(expected, actual);
  }

  private static void assertSizeEqual(Iterator<Document> documents, String filePath)
      throws IOException {
    String fileContent = readFileFromResource(filePath).orElseThrow();
    int expected = convertJsonToMap(fileContent).size();
    int actual;

    for (actual = 0; documents.hasNext(); actual++) {
      documents.next();
    }

    Assertions.assertEquals(expected, actual);
  }
}
