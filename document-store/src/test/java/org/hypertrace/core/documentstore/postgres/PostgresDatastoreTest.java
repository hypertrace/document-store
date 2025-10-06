package org.hypertrace.core.documentstore.postgres;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.DatabaseType;
import org.hypertrace.core.documentstore.model.config.DatastoreConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.postgresql.PGProperty;

@ExtendWith(MockitoExtension.class)
class PostgresDatastoreTest {

  @Mock private Connection mockConnection;

  @Test
  void initWithHostAndPort() {
    final Config config =
        ConfigFactory.parseMap(
            Map.ofEntries(
                entry("database", "inputDB"),
                entry("host", "localhost"),
                entry("port", "5432"),
                entry("user", "u1"),
                entry("password", "pass"),
                entry("maxConnectionAttempts", "7"),
                entry("applicationName", "app1"),
                entry("connectionRetryBackoff", "2 minutes")));
    PostgresDatastore datastore =
        (PostgresDatastore) DatastoreProvider.getDatastore("postgres", config);

    final Properties properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), "u1");
    properties.setProperty(PGProperty.PASSWORD.getName(), "pass");
    properties.setProperty(PGProperty.APPLICATION_NAME.getName(), "app1");

    try (final MockedStatic<DriverManager> driverManager =
        Mockito.mockStatic(DriverManager.class)) {
      driverManager
          .when(
              () ->
                  DriverManager.getConnection(
                      "jdbc:postgresql://localhost:5432/inputDB", properties))
          .thenReturn(mockConnection);
      assertEquals(mockConnection, datastore.getPostgresClient());
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DocumentTypeProvider.class)
  @MockitoSettings(strictness = Strictness.LENIENT)
  void testGetCollectionForDocTypes(DocumentType documentType) throws SQLException {
    try (final MockedStatic<DriverManager> driverManager =
        Mockito.mockStatic(DriverManager.class)) {
      driverManager
          .when(() -> DriverManager.getConnection(any(), any()))
          .thenReturn(mockConnection);
      Datastore datastore =
          DatastoreProvider.getDatastore(
              DatastoreConfig.builder()
                  .type(DatabaseType.POSTGRES)
                  .connectionConfig(
                      ConnectionConfig.builder()
                          .type(DatabaseType.POSTGRES)
                          .database("inputDB")
                          .build())
                  .build());
      DatabaseMetaData mockMetadata = mock(DatabaseMetaData.class);
      PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
      when(mockMetadata.getTables(any(), any(), any(), any())).thenReturn(mock(ResultSet.class));
      when(mockConnection.getMetaData()).thenReturn(mockMetadata);
      when(mockConnection.prepareStatement(any())).thenReturn(mockPreparedStatement);
      datastore.getCollectionForType("testSchema.testTable", documentType);

      // For FLAT types, we don't create schema and collection
      verify(mockConnection, times(documentType == DocumentType.NESTED ? 1 : 0))
          .prepareStatement("CREATE SCHEMA IF NOT EXISTS testSchema;");
      verify(mockConnection, times(documentType == DocumentType.NESTED ? 1 : 0))
          .prepareStatement(
              "CREATE TABLE testSchema.\"testTable\" (id TEXT PRIMARY KEY,document jsonb NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());");
    }
  }

  @Test
  void testDeleteExplicitSchemaTable() throws SQLException {
    try (final MockedStatic<DriverManager> driverManager =
        Mockito.mockStatic(DriverManager.class)) {
      driverManager
          .when(() -> DriverManager.getConnection(any(), any()))
          .thenReturn(mockConnection);
      Datastore datastore =
          DatastoreProvider.getDatastore(
              DatastoreConfig.builder()
                  .type(DatabaseType.POSTGRES)
                  .connectionConfig(
                      ConnectionConfig.builder()
                          .type(DatabaseType.POSTGRES)
                          .database("inputDB")
                          .build())
                  .build());
      PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
      when(mockConnection.prepareStatement(any())).thenReturn(mockPreparedStatement);
      datastore.deleteCollection("testSchema.testTable");

      verify(mockConnection, times(1))
          .prepareStatement("DROP TABLE IF EXISTS testSchema.\"testTable\"");
    }
  }

  private static class DocumentTypeProvider implements ArgumentsProvider {

    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(Arguments.of(DocumentType.FLAT), Arguments.of(DocumentType.NESTED));
    }
  }
}
