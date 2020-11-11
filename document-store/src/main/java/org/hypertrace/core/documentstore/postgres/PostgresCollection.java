package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class PostgresCollection implements Collection {
  
  private static final String ID_KEY = "id";
  private static final String DOCUMENT_KEY = "document";
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCollection.class);
  private static final String UPDATED_AT = "updated_at";
  private static final String CREATED_AT = "created_at";
  private static final Set<String> OUTER_COLUMNS = new HashSet<>() {{
    add(CREATED_AT);
    add(ID_KEY);
    add(UPDATED_AT);
  }};
  
  private Connection client;
  private String collectionName;
  private String columnType;
  
  public PostgresCollection(Connection client, String collectionName, String columnType) {
    this.client = client;
    this.collectionName = collectionName;
    this.columnType = columnType;
  }
  
  @Override
  public boolean upsert(Key key, Document document) {
    String jsonString = document.toJson();
    String upsertSQL = String.format("INSERT INTO %s (%s,%s) VALUES( ?, ? :: %s) ON CONFLICT(%s) DO UPDATE SET %s = ?::%s ",
      collectionName, ID_KEY, DOCUMENT_KEY, columnType, ID_KEY, DOCUMENT_KEY, columnType);
    try {
      
      PreparedStatement preparedStatement = client.prepareStatement(upsertSQL, Statement.RETURN_GENERATED_KEYS);
      preparedStatement.setString(1, key.toString());
      preparedStatement.setString(2, jsonString);
      preparedStatement.setString(3, jsonString);
      ResultSet resultSet = preparedStatement.executeQuery();
      
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: " + resultSet.toString());
      }
      
      return resultSet.rowUpdated();
    } catch (SQLException e) {
      LOGGER.error("SQLException inserting document. key: {} content:{}", key, document, e);
    }
    return false;
  }
  
  @Override
  public boolean updateSubDoc(Key key, String subDocPath, Document subDocument) {
    return false;
  }
  
  @Override
  public Iterator<Document> search(Query query) {
    String filters = null;
    String space = " ";
    StringBuilder searchSQLBuilder = new StringBuilder("SELECT * FROM")
      .append(space).append(collectionName);
    
    // If there is a filter in the query, parse it fully.
    if (query.getFilter() != null) {
      filters = parseQuery(query.getFilter());
    }
    
    LOGGER.debug(
      "Sending query to PostgresSQL: {} : {}",
      collectionName,
      filters);
    
    if (filters != null) {
      searchSQLBuilder
        .append(" WHERE ").append(filters);
    }
    
    if (!query.getOrderBys().isEmpty()) {
      String orderBySQL = parseOrderByQuery(query.getOrderBys());
      searchSQLBuilder.append(" ORDER BY ").append(orderBySQL);
    }
    
    Integer limit = query.getLimit();
    if (limit != null && limit >= 0) {
      searchSQLBuilder.append(" LIMIT ").append(limit);
    }
    
    Integer offset = query.getOffset();
    if (offset != null && offset >= 0) {
      searchSQLBuilder.append(" OFFSET ").append(offset);
    }
    
    try {
      PreparedStatement preparedStatement = client.prepareStatement(searchSQLBuilder.toString());
      ResultSet resultSet = preparedStatement.executeQuery();
      return new PostgresResultIterator(resultSet);
    } catch (SQLException e) {
      LOGGER.error("SQLException querying documents. query: {}", query, e);
    }
    
    return null;
  }
  
  @VisibleForTesting
  /**
   *
   */
  String parseQuery(Filter filter) {
    if (filter.isComposite()) {
      Filter.Op op = filter.getOp();
      switch (op) {
        case OR: {
          String childList =
            Arrays.stream(filter.getChildFilters())
              .map(this::parseQuery)
              .filter(str -> !str.isEmpty())
              .collect(Collectors.joining(" OR "));
          if (!childList.isEmpty()) {
            return childList;
          } else {
            return null;
          }
        }
        case AND: {
          String childList =
            Arrays.stream(filter.getChildFilters())
              .map(this::parseQuery)
              .filter(str -> !str.isEmpty())
              .collect(Collectors.joining(" AND "));
          if (!childList.isEmpty()) {
            return childList;
          } else {
            return null;
          }
        }
        default:
          throw new UnsupportedOperationException(
            String.format("Boolean operation:%s not supported", op));
      }
    } else {
      Filter.Op op = filter.getOp();
      Object value = filter.getValue();
      String fieldName = filter.getFieldName();
      String prefix = getFieldPrefix(fieldName);
      StringBuilder filterString = new StringBuilder(prefix);
      switch (op) {
        case EQ:
          filterString.append(" = ");
          break;
        case LIKE:
          // Case insensitive regex search, Append % at beginning and end of value to do a regex search
          filterString.append(" ILIKE ");
          value = "%" + value + "%";
          break;
        case IN:
          filterString.append(" IN ");
          break;
        case CONTAINS:
          // TODO: Matches condition inside an array of documents
          break;
        case GT:
          filterString.append(" > ");
          break;
        case LT:
          filterString.append(" < ");
          break;
        case GTE:
          filterString.append(" >= ");
          break;
        case LTE:
          filterString.append(" <= ");
          break;
        case EXISTS:
          // TODO: Checks if key exists
          break;
        case NOT_EXISTS:
          // TODO: Checks if key does not exist
          break;
        case AND:
        case NEQ:
        case OR:
          throw new UnsupportedOperationException("Only Equality predicate is supported");
        default:
          break;
      }
      return filterString.append("'" + value + "'").toString();
    }
  }
  
  /**
   * If the query column is a json column, then add document column prefix to it\
   * This will currently only search first level keys inside JSON
   * TODO: Add support for deep nested keys and arrays
   */
  private String getFieldPrefix(String fieldName) {
    String fieldPrefix = fieldName;
    if (!OUTER_COLUMNS.contains(fieldName)) {
      fieldPrefix = DOCUMENT_KEY + "->>" + "'" + fieldName + "'";
    }
    return fieldPrefix;
  }
  
  private String parseOrderByQuery(List<OrderBy> orderBys) {
    String orderBySQL = orderBys
      .stream()
      .map(orderBy -> orderBy.getField() + " " + (orderBy.isAsc() ? "ASC" : "DESC"))
      .filter(str -> !str.isEmpty())
      .collect(Collectors.joining(" , "));
    
    return orderBySQL;
  }
  
  @Override
  public boolean delete(Key key) {
    String deleteSQL = String.format("DELETE FROM %s WHERE %s = ?", collectionName, ID_KEY);
    try {
      PreparedStatement preparedStatement = client.prepareStatement(deleteSQL);
      preparedStatement.setString(1, key.toString());
      preparedStatement.executeUpdate();
      return true;
    } catch (SQLException e) {
      LOGGER.error("SQLException deleting document. key: {}", key, e);
    }
    return false;
  }
  
  @Override
  public boolean deleteSubDoc(Key key, String subDocPath) {
    return false;
  }
  
  @Override
  public boolean deleteAll() {
    String deleteSQL = String.format("DELETE FROM %s", collectionName);
    try {
      PreparedStatement preparedStatement = client.prepareStatement(deleteSQL);
      preparedStatement.executeUpdate();
      return true;
    } catch (SQLException e) {
      LOGGER.error("SQLException deleting all documents.", e);
    }
    return false;
  }
  
  @Override
  public long count() {
    String countSQL = String.format("SELECT COUNT(*) FROM %s", collectionName);
    try {
      PreparedStatement preparedStatement = client.prepareStatement(countSQL);
      ResultSet resultSet = preparedStatement.executeQuery();
      return resultSet.getLong(1);
    } catch (SQLException e) {
      LOGGER.error("SQLException deleting all documents.", e);
    }
    return 0;
  }
  
  @Override
  public long total(Query query) {
    StringBuilder totalSQLBuilder = new StringBuilder("SELECT COUNT(*) FROM ").append(collectionName);
    long count = 0;
    // If there is a filter in the query, parse it fully.
    if (query.getFilter() != null) {
      totalSQLBuilder
        .append(" WHERE ")
        .append(parseQuery(query.getFilter()));
    }
    
    try {
      PreparedStatement preparedStatement = client.prepareStatement(totalSQLBuilder.toString());
      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) count = resultSet.getLong(1);
    } catch (SQLException e) {
      LOGGER.error("SQLException querying documents. query: {}", query, e);
    }
    return count;
  }
  
  @Override
  public boolean bulkUpsert(Map<Key, Document> documents) {
    return false;
  }
  
  @Override
  public void drop() {
    String dropTableSQL = String.format("DROP TABLE IF EXISTS %s", collectionName);
    try {
      PreparedStatement preparedStatement = client.prepareStatement(dropTableSQL);
      preparedStatement.executeUpdate();
      preparedStatement.close();
    } catch (SQLException e) {
      LOGGER.error("Exception deleting table name: {}", collectionName);
    }
  }
  
  class PostgresResultIterator implements Iterator {
    
    private final ObjectMapper MAPPER = new ObjectMapper();
    ResultSet resultSet;
    
    public PostgresResultIterator(ResultSet resultSet) {
      this.resultSet = resultSet;
    }
    
    @Override
    public boolean hasNext() {
      try {
        return resultSet.next();
      } catch (SQLException e) {
        LOGGER.error("SQLException iterating documents.", e);
      }
      return false;
    }
    
    @Override
    public Document next() {
      try {
        String documentString = resultSet.getString(DOCUMENT_KEY);
        ObjectNode jsonNode = (ObjectNode) MAPPER.readTree(documentString);
        
        // Add Timestamps to Document
        Timestamp createdAt = resultSet.getTimestamp(CREATED_AT);
        Timestamp updatedAt = resultSet.getTimestamp(UPDATED_AT);
        jsonNode.put(CREATED_AT, String.valueOf(createdAt));
        jsonNode.put(UPDATED_AT, String.valueOf(updatedAt));
        
        return new JSONDocument(MAPPER.writeValueAsString(jsonNode));
      } catch (IOException | SQLException e) {
        return JSONDocument.errorDocument(e.getMessage());
      }
    }
    
  }
}

