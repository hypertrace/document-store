package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.utils.Utils.createDocumentsFromResource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.documentstore.mongo.MongoCollection;
import org.hypertrace.core.documentstore.mongo.MongoDatastore;
import org.hypertrace.core.documentstore.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.com.google.common.collect.Maps;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.utility.DockerImageName;

public class PostgresExecutorIntegrationTest {
  public static final String POSTGRES_STORE = "Postgres";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String COLLECTION_NAME = "myTest";

  /** Postgres related time fields */
  public static final String POSTGRES_UPDATED_AT = "updated_at";

  public static final String POSTGRES_CREATED_AT = "created_at";

  private static Map<String, Datastore> datastoreMap;
  private static GenericContainer<?> postgres;
  private static Collection collection;

  @BeforeAll
  public static void init() {
    datastoreMap = Maps.newHashMap();
    postgres =
        new GenericContainer<>(DockerImageName.parse("postgres:13.1"))
            .withEnv("POSTGRES_PASSWORD", "postgres")
            .withEnv("POSTGRES_USER", "postgres")
            .withExposedPorts(5432)
            .waitingFor(Wait.forListeningPort());
    postgres.start();

    String postgresConnectionUrl =
        String.format("jdbc:postgresql://localhost:%s/", postgres.getMappedPort(5432));
    DatastoreProvider.register("POSTGRES", PostgresDatastore.class);

    Map<String, String> postgresConfig = new HashMap<>();
    postgresConfig.putIfAbsent("url", postgresConnectionUrl);
    postgresConfig.putIfAbsent("user", "postgres");
    postgresConfig.putIfAbsent("password", "postgres");
    Datastore postgresDatastore =
        DatastoreProvider.getDatastore("Postgres", ConfigFactory.parseMap(postgresConfig));
    System.out.println(postgresDatastore.listCollections());
    datastoreMap.put(POSTGRES_STORE, postgresDatastore);

  }

  @AfterEach
  public void cleanup() {
    datastoreMap.forEach(
        (k, v) -> {
          v.deleteCollection(COLLECTION_NAME);
          v.createCollection(COLLECTION_NAME, null);
        });
  }

  @AfterAll
  public static void shutdown() {
    postgres.stop();
  }

  private static Map<Key, Document> createDocumentsFromResource(String resourcePath)
      throws IOException {
    Optional<String> contentOptional = readFileFromResource(resourcePath);
    String json = contentOptional.orElseThrow();
    List<Map<String, Object>> maps = OBJECT_MAPPER.readValue(json, new TypeReference<>() {});

    Map<Key, Document> documentMap = new HashMap<>();
    for (Map<String, Object> map : maps) {
      Key key = new SingleValueKey("default", map.get(MongoCollection.ID_KEY).toString());
      Document value = new JSONDocument(map);
      documentMap.put(key, value);
    }

    return documentMap;
  }

  public static Optional<String> readFileFromResource(String filePath) throws IOException {
    ClassLoader classLoader = Utils.class.getClassLoader();

    try (InputStream inputStream = classLoader.getResourceAsStream(filePath)) {
      // the stream holding the file content
      if (inputStream == null) {
        throw new IllegalArgumentException("Resource not found: " + filePath);
      }

      return Optional.ofNullable(IOUtils.toString(inputStream, StandardCharsets.UTF_8));
    }
  }

  @Test
  public void basicGroupByTest() throws IOException {
    Map<Key, Document> documents = createDocumentsFromResource("mongo/collection_data.json");
    Datastore datastore = datastoreMap.get(POSTGRES_STORE);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    boolean result = collection.bulkUpsert(documents);
    Assertions.assertTrue(result);
  }

}
