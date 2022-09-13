package org.hypertrace.core.documentstore.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest.Operation;
import org.hypertrace.core.documentstore.BulkDeleteResult;
import org.hypertrace.core.documentstore.BulkUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateResult;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.CreateResult;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.OrderBy;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.UpdateResult;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PostgresCollectionTest {

  @Mock
  private PostgresClient mockClient;

  private PostgresCollection postgresCollection;

  @BeforeEach
  void setUp() {
    postgresCollection = new PostgresCollection(mockClient, "collection");
  }

  @Test
  void testUpdate2() throws Exception {
    // Setup
    final org.hypertrace.core.documentstore.query.Query query = org.hypertrace.core.documentstore.query.Query.builder()
        .build();
    final Collection<SubDocumentUpdate> updates = List.of(
        SubDocumentUpdate.of("subDocumentPath", "value"));

    // Configure PostgresClient.getNewConnection(...).
    final Connection mockConnection = mock(Connection.class);
    when(mockClient.getNewConnection()).thenReturn(mockConnection);

    // Run the test
    final Optional<Document> result = postgresCollection.update(query, updates);

    // Verify the results
    verify(mockConnection).close();
  }

  @Test
  void testUpdate2_PostgresClientThrowsSQLException() throws Exception {
    // Setup
    final org.hypertrace.core.documentstore.query.Query query = org.hypertrace.core.documentstore.query.Query.builder()
        .build();
    final Collection<SubDocumentUpdate> updates = List.of(
        SubDocumentUpdate.of("subDocumentPath", "value"));
    when(mockClient.getNewConnection()).thenThrow(SQLException.class);

    // Run the test
    final Optional<Document> result = postgresCollection.update(query, updates);

    // Verify the results
  }
}
