package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.OrderBy;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PostgresDocStoreTest {

    public static final String ID = "id";
    public static final String DOCUMENT_ID = "_id";
    public static final String UPDATED_AT = "updated_at";
    public static final String CREATED_AT = "created_at";
    private static final String COLLECTION_NAME = "mytest";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static Datastore datastore;


    @BeforeAll
    public static void init() {

        DatastoreProvider.register("POSTGRES", PostgresDatastore.class);

        Map<String, String> postgresConfig = new HashMap<>();
        postgresConfig.putIfAbsent("url", "jdbc:postgresql://localhost:5432/");
        postgresConfig.putIfAbsent("user", "postgres");
        postgresConfig.putIfAbsent("password", "postgres");
        Config config = ConfigFactory.parseMap(postgresConfig);

        datastore = DatastoreProvider.getDatastore("Postgres", config);
        System.out.println(datastore.listCollections());
    }

    @BeforeEach
    public void setUp() {
        datastore.deleteCollection(COLLECTION_NAME);
        datastore.createCollection(COLLECTION_NAME, null);
    }

    @Test
    public void testInitWithDatabase() {
        PostgresDatastore datastore = new PostgresDatastore();
        Properties properties = new Properties();
        String url = "jdbc:postgresql://localhost:5432/";
        String user = "postgres";
        String password = "postgres";
        String database = "postgres";

        properties.put("url", url);
        properties.put("user", user);
        properties.put("password", password);
        properties.put("database", database);
        Config config = ConfigFactory.parseProperties(properties);
        datastore.init(config);

        try {
            DatabaseMetaData metaData = datastore.getPostgresClient().getMetaData();
            Assertions.assertEquals(metaData.getURL(), url + database);
            Assertions.assertEquals(metaData.getUserName(), user);
        } catch (SQLException e) {
            System.out.println("Exception executing init test with user and password");
            Assertions.fail();
        }

    }

    @Test
    public void testUpsert() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        boolean result = collection
                .upsert(new SingleValueKey("default", "testKey"), createDocument("foo1", "bar1"));
        Assertions.assertTrue(result);

        Query query = new Query();
        query.setFilter(Filter.eq(ID, "default:testKey"));
        Iterator<Document> results = collection.search(query);
        List<Document> documents = new ArrayList<>();
        while (results.hasNext()) {
            documents.add(results.next());
        }
        Assertions.assertFalse(documents.isEmpty());
        String persistedDocument = documents.get(0).toJson();
        Assertions.assertTrue(persistedDocument.contains(UPDATED_AT));
        Assertions.assertTrue(persistedDocument.contains(CREATED_AT));
    }

    @Test
    public void testUpsertAndReturn() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        Document document = createDocument("foo1", "bar1");
        Document resultDocument = collection
                .upsertAndReturn(new SingleValueKey("default", "testKey"), document);

        Assertions.assertEquals(document.toJson(), resultDocument.toJson());
    }

    @Test
    public void testBulkUpsert() {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        Map<Key, Document> bulkMap = new HashMap<>();
        bulkMap.put(new SingleValueKey("default", "testKey1"), createDocument("name", "Bob"));
        bulkMap.put(new SingleValueKey("default", "testKey2"), createDocument("name", "Alice"));
        bulkMap.put(new SingleValueKey("default", "testKey3"), createDocument("name", "Alice"));
        bulkMap.put(new SingleValueKey("default", "testKey4"), createDocument("name", "Bob"));
        bulkMap.put(new SingleValueKey("default", "testKey5"), createDocument("name", "Alice"));
        bulkMap.put(
                new SingleValueKey("default", "testKey6"), createDocument("email", "bob@example.com"));

        collection.bulkUpsert(bulkMap);

        {
            // empty query returns all the documents
            Query query = new Query();
            Assertions.assertEquals(6, collection.total(query));
        }

        {
            Query query = new Query();
            query.setFilter(Filter.eq("name", "Bob"));
            Assertions.assertEquals(2, collection.total(query));
        }

        {
            // limit should not affect the total
            Query query = new Query();
            query.setFilter(Filter.eq("name", "Bob"));
            query.setLimit(1);
            Assertions.assertEquals(2, collection.total(query));
        }
    }

    @Test
    public void testBulkUpsertAndReturn() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        Map<Key, Document> bulkMap = new HashMap<>();
        bulkMap.put(new SingleValueKey("default", "testKey1"), createDocument("name", "Bob"));
        bulkMap.put(new SingleValueKey("default", "testKey2"), createDocument("name", "Alice"));
        bulkMap.put(new SingleValueKey("default", "testKey3"), createDocument("name", "Alice"));
        bulkMap.put(new SingleValueKey("default", "testKey4"), createDocument("name", "Bob"));
        bulkMap.put(new SingleValueKey("default", "testKey5"), createDocument("name", "Alice"));
        bulkMap.put(
                new SingleValueKey("default", "testKey6"), createDocument("email", "bob@example.com"));

        Iterator<Document> iterator = collection.bulkUpsertAndReturnOlderDocuments(bulkMap);
        // Initially there shouldn't be any documents.
        Assertions.assertFalse(iterator.hasNext());

        // The operation should be idempotent, so go ahead and try again.
        iterator = collection.bulkUpsertAndReturnOlderDocuments(bulkMap);
        List<Document> documents = new ArrayList<>();
        while (iterator.hasNext()) {
            documents.add(iterator.next());
        }
        Assertions.assertEquals(6, documents.size());

        {
            // empty query returns all the documents
            Query query = new Query();
            Assertions.assertEquals(6, collection.total(query));
        }

        {
            Query query = new Query();
            query.setFilter(Filter.eq("name", "Bob"));
            Assertions.assertEquals(2, collection.total(query));
        }

        {
            // limit should not affect the total
            Query query = new Query();
            query.setFilter(Filter.eq("name", "Bob"));
            query.setLimit(1);
            Assertions.assertEquals(2, collection.total(query));
        }
    }

    @Test
    public void testSubDocumentUpdate() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        SingleValueKey docKey = new SingleValueKey("default", "testKey");
        collection.upsert(docKey, createDocument("foo1", "bar1"));

        Document subDocument = createDocument("subfoo1", "subbar1");
        collection.updateSubDoc(docKey, "subdoc", subDocument);

        Document nestedDocument = createDocument("nestedfoo1", "nestedbar1");
        collection.updateSubDoc(docKey, "subdoc.nesteddoc", nestedDocument);

        Query query = new Query();
        query.setFilter(Filter.eq(ID, "default:testKey"));
        Iterator<Document> results = collection.search(query);
        List<Document> documents = new ArrayList<>();
        while (results.hasNext()) {
            documents.add(results.next());
        }
        Assertions.assertFalse(documents.isEmpty());

        ObjectNode jsonNode = (ObjectNode) OBJECT_MAPPER.readTree(documents.get(0).toJson());
        jsonNode.remove(CREATED_AT);
        jsonNode.remove(UPDATED_AT);
        String expected = "{\"foo1\":\"bar1\",\"subdoc\":{\"subfoo1\":\"subbar1\",\"nesteddoc\":{\"nestedfoo1\":\"nestedbar1\"}}}";
        Assertions.assertEquals(expected, OBJECT_MAPPER.writeValueAsString(jsonNode));
    }

    @Test
    public void testSubDocumentDelete() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        SingleValueKey docKey = new SingleValueKey("default", "testKey");
        collection.upsert(docKey, createDocument("foo1", "bar1"));

        Document subDocument = createDocument("subfoo1", "subbar1");
        collection.updateSubDoc(docKey, "subdoc", subDocument);

        boolean status = collection.deleteSubDoc(docKey, "subdoc.subfoo1");
        Assertions.assertTrue(status);

        status = collection.deleteSubDoc(docKey, "subdoc");
        Assertions.assertTrue(status);
    }

    @Test
    public void testCount() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        SingleValueKey docKey = new SingleValueKey("default", "testKey");
        collection.upsert(docKey, createDocument("foo1", "bar1"));
        Assertions.assertEquals(collection.count(), 1);
    }

    @Test
    public void testDelete() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        SingleValueKey docKey = new SingleValueKey("default", "testKey");
        collection.upsert(docKey, createDocument("foo1", "bar1"));

        Assertions.assertEquals(collection.count(), 1);
        collection.delete(docKey);
        Assertions.assertEquals(collection.count(), 0);

    }

    @Test
    public void testDeleteAll() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        SingleValueKey docKey = new SingleValueKey("default", "testKey");
        collection.upsert(docKey, createDocument("foo1", "bar1"));

        Assertions.assertEquals(collection.count(), 1);
        collection.deleteAll();
        Assertions.assertEquals(collection.count(), 0);
    }

    @Test
    public void testDrop() {
        Collection collection = datastore.getCollection(COLLECTION_NAME);

        Assertions.assertTrue(datastore.listCollections().contains("postgres." + COLLECTION_NAME));
        collection.drop();
        Assertions.assertFalse(datastore.listCollections().contains("postgres." + COLLECTION_NAME));
    }

    @Test
    public void testIgnoreCaseLikeQuery() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        collection.upsert(new SingleValueKey("default", "testKey"), createDocument("name", "Bob"));

        String[] ignoreCaseSearchValues = {"Bob", "bob", "BOB", "bOB", "BO", "bO", "Ob", "OB"};

        for (String searchValue : ignoreCaseSearchValues) {
            Query query = new Query();
            query.setFilter(new Filter(Filter.Op.LIKE, "name", searchValue));
            Iterator<Document> results = collection.search(query);
            List<Document> documents = new ArrayList<>();
            while (results.hasNext()) {
                documents.add(results.next());
            }
            Assertions.assertFalse(documents.isEmpty());
            String persistedDocument = documents.get(0).toJson();
            Assertions.assertTrue(persistedDocument.contains("Bob"));
        }
    }

    @Test
    public void testSearch() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        String documentString = "{\"attributes\":{\"trace_id\":{\"value\":{\"string\":\"00000000000000005e194fdf9fbf5101\"}},\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},\"FQN\":{\"value\":{\"string\":\"driver\"}}},\"createdTime\":1605692185945,\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\",\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string\":\"driver\"}}},\"tenantId\":\"__default\"}";
        Document document = new JSONDocument(documentString);
        SingleValueKey key = new SingleValueKey("default", "testKey1");
        collection.upsert(key, document);

        // Search _id field in the document
        Query query = new Query();
        query.setFilter(new Filter(Filter.Op.EQ, DOCUMENT_ID, key.toString()));
        Iterator<Document> results = collection.search(query);
        List<Document> documents = new ArrayList<>();
        while (results.hasNext()) {
            documents.add(results.next());
        }
        Assertions.assertEquals(documents.size(), 1);
    }

    @Test
    public void testSearchForNestedKey() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        String documentString = "{\"attributes\":{\"trace_id\":{\"value\":{\"string\":\"00000000000000005e194fdf9fbf5101\"}},\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},\"FQN\":{\"value\":{\"string\":\"driver\"}}},\"createdTime\":1605692185945,\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\",\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string\":\"driver\"}}},\"tenantId\":\"__default\"}";
        Document document = new JSONDocument(documentString);
        SingleValueKey key = new SingleValueKey("default", "testKey1");
        collection.upsert(key, document);

        // Search nested field in the document
        Query query = new Query();
        query
                .setFilter(new Filter(Filter.Op.EQ, "attributes.span_id.value.string", "6449f1f720c93a67"));
        Iterator<Document> results = collection.search(query);
        List<Document> documents = new ArrayList<>();
        while (results.hasNext()) {
            documents.add(results.next());
        }
        Assertions.assertEquals(documents.size(), 1);
    }

    @Test
    public void testTotalWithQuery() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        collection.upsert(new SingleValueKey("default", "testKey1"), createDocument("name", "Bob"));
        collection.upsert(new SingleValueKey("default", "testKey2"), createDocument("name", "Alice"));
        collection.upsert(new SingleValueKey("default", "testKey3"), createDocument("name", "Alice"));
        collection.upsert(new SingleValueKey("default", "testKey4"), createDocument("name", "Bob"));
        collection.upsert(new SingleValueKey("default", "testKey5"), createDocument("name", "Alice"));
        collection.upsert(
                new SingleValueKey("default", "testKey6"), createDocument("email", "bob@example.com"));

        {
            // empty query returns all the documents
            Query query = new Query();
            Assertions.assertEquals(6, collection.total(query));
        }

        {
            Query query = new Query();
            query.setFilter(Filter.eq("name", "Bob"));
            Assertions.assertEquals(2, collection.total(query));
        }

        {
            // limit should not affect the total
            Query query = new Query();
            query.setFilter(Filter.eq("name", "Bob"));
            query.setLimit(1);
            Assertions.assertEquals(2, collection.total(query));
        }
    }

    @Test
    public void testOffsetLimitAndOrderBY() throws IOException {
        Collection collection = datastore.getCollection(COLLECTION_NAME);
        collection.upsert(new SingleValueKey("default", "testKey1"), createDocument("foo1", "bar1"));
        collection.upsert(new SingleValueKey("default", "testKey2"), createDocument("foo2", "bar2"));
        collection.upsert(new SingleValueKey("default", "testKey3"), createDocument("foo3", "bar3"));
        collection.upsert(new SingleValueKey("default", "testKey4"), createDocument("foo4", "bar4"));
        collection.upsert(new SingleValueKey("default", "testKey5"), createDocument("foo5", "bar5"));

        // Querying 5 times, to make sure the order of results is maintained with offset + limit
        for (int i = 0; i < 5; i++) {
            Query query = new Query();
            query.setLimit(2);
            query.setOffset(1);
            query.addOrderBy(new OrderBy(DOCUMENT_ID, true));

            Iterator<Document> results = collection.search(query);
            List<Document> documents = new ArrayList<>();
            while (results.hasNext()) {
                documents.add(results.next());
            }

            Assertions.assertEquals(2, documents.size());
            String persistedDocument1 = documents.get(0).toJson();
            Assertions.assertTrue(persistedDocument1.contains("foo2"));
            String persistedDocument2 = documents.get(1).toJson();
            Assertions.assertTrue(persistedDocument2.contains("foo3"));
        }
    }


    private Document createDocument(String key, String value) {
        ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
        objectNode.put(key, value);
        return new JSONDocument(objectNode);
    }

}
