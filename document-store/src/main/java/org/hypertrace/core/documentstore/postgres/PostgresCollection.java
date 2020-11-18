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
  
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCollection.class);
  private final ObjectMapper MAPPER = new ObjectMapper();
  
  private String DOC_PATH_SEPARATOR = "\\.";
  
  public static final String ID = "id";
  public static final String DOCUMENT_ID = "_id";
  public static final String DOCUMENT = "document";
  public static final String UPDATED_AT = "updated_at";
  public static final String CREATED_AT = "created_at";
  public static final Set<String> OUTER_COLUMNS = new HashSet<>() {{
    add(CREATED_AT);
    add(ID);
    add(UPDATED_AT);
  }};
  private Connection client;
  private String collectionName;
  
  public PostgresCollection(Connection client, String collectionName) {
    this.client = client;
    this.collectionName = collectionName;
  }
  
  @Override
  public boolean upsert(Key key, Document document) throws IOException {
    
    try {
      PreparedStatement preparedStatement = client.prepareStatement(getUpsertSQL(), Statement.RETURN_GENERATED_KEYS);
      String jsonString = prepareUpsertDocument(key, document);
      
      preparedStatement.setString(1, key.toString());
      preparedStatement.setString(2, jsonString);
      preparedStatement.setString(3, jsonString);
      
      int result = preparedStatement.executeUpdate();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: " + result);
      }
      return result >= 0;
    } catch (SQLException e) {
      LOGGER.error("SQLException inserting document. key: {} content:{}", key, document, e);
      throw new IOException(e);
    }
  }
  
  @Override
  /**
   * For Postgres upsertAndReturn functionality is not supported directly.
   * So using direct upsert and then return document.
   */
  public Document upsertAndReturn(Key key, Document document) throws IOException {
    try {
      boolean upsert = upsert(key, document);
      return upsert ? document : null;
    } catch (IOException e) {
      LOGGER.error("SQLException inserting document. key: {} content:{}", key, document, e);
      throw e;
    }
  }
  
  @Override
  /**
   * This function will replace the complete sub document at subDocPath. If the key doesn't exist then it will create the key,
   * only if the subDocPath exists before the last level of the key. i.e subdoc1.subdoc2, the json document should have key subdoc1.
   * In case of nested subDocPath, if the nested path doesn't exist inside json document, this function won't update anything.
   * Example: Document: {"foo":"bar"}
   * SubDocPath: "subdoc1.subdoc2"
   * SubDocument: {"subdoc3": "subdoc3val"}
   * As the path subdoc1.subdoc2 doesn't exist, this wouldn't update anything.
   * But when the following specifications are provided:
   * SubDocPath: "subdoc1"
   * SubDocument: {"subdoc2": {"subdoc3": "subdoc3Val"}}
   * This will create the key subdoc1, and insert the document in respect to the key.
   */
  public boolean updateSubDoc(Key key, String subDocPath, Document subDocument) {
    String updateSubDocSQL = String.format("UPDATE %s SET %s=jsonb_set(%s, ?::text[], ?::jsonb) WHERE %s=?",
      collectionName, DOCUMENT, DOCUMENT, ID);
    String jsonSubDocPath = getJsonSubDocPath(subDocPath);
    String jsonString = subDocument.toJson();
    try {
      
      PreparedStatement preparedStatement = client.prepareStatement(updateSubDocSQL, Statement.RETURN_GENERATED_KEYS);
      preparedStatement.setString(1, jsonSubDocPath);
      preparedStatement.setString(2, jsonString);
      preparedStatement.setString(3, key.toString());
      int resultSet = preparedStatement.executeUpdate();
      
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: " + resultSet);
      }
      
      return true;
    } catch (SQLException e) {
      LOGGER.error("SQLException updating sub document. key: {} subDocPath: {} content:{}", key, subDocPath, subDocument, e);
    }
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
  protected String parseQuery(Filter filter) {
    if (filter.isComposite()) {
      return parseQueryForCompositeFilter(filter);
    } else {
      return parseQueryForNonCompositeFilter(filter);
    }
  }
  
  @VisibleForTesting
  protected String parseQueryForNonCompositeFilter(Filter filter) {
    Filter.Op op = filter.getOp();
    Object value = filter.getValue();
    String fieldName = filter.getFieldName();
    String prefix = getFieldPrefix(fieldName);
    StringBuilder filterString = new StringBuilder(prefix);
    switch (op) {
      case EQ:
        filterString.append(" = ");
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
      case LIKE:
        // Case insensitive regex search, Append % at beginning and end of value to do a regex search
        filterString.append(" ILIKE ");
        value = "%" + value + "%";
        break;
      case IN:
        filterString.append(" IN ");
        List<Object> values = (List<Object>) value;
        String collect = values
          .stream()
          .map(val -> "'" + val + "'")
          .collect(Collectors.joining(", "));
        return filterString.append("(" + collect + ")").toString();
      case CONTAINS:
        // TODO: Matches condition inside an array of documents
      case EXISTS:
        // TODO: Checks if key exists
      case NOT_EXISTS:
        // TODO: Checks if key does not exist
      case NEQ:
        throw new UnsupportedOperationException("Only Equality predicate is supported");
      default:
        throw new UnsupportedOperationException(
          String.format("Query operation:%s not supported", op));
    }
    return filterString.append("'").append(value).append("'").toString();
  }
  
  @VisibleForTesting
  protected String parseQueryForCompositeFilter(Filter filter) {
    Filter.Op op = filter.getOp();
    switch (op) {
      case OR: {
        String childList =
          Arrays.stream(filter.getChildFilters())
            .map(this::parseQuery)
            .filter(str -> !str.isEmpty())
            .map(str -> "(" + str + ")")
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
            .map(str -> "(" + str + ")")
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
  }
  
  @VisibleForTesting
  private String getJsonSubDocPath(String subDocPath) {
    return "{" + subDocPath.replaceAll(DOC_PATH_SEPARATOR, ",") + "}";
  }
  
  /**
   * If the query column is a json column, then add document column prefix to it\
   * This will currently only search first level keys inside JSON
   * TODO: Add support for deep nested keys and arrays
   */
  private String getFieldPrefix(String fieldName) {
    StringBuilder fieldPrefix = new StringBuilder(fieldName);
    if (!OUTER_COLUMNS.contains(fieldName)) {
      fieldPrefix = new StringBuilder(DOCUMENT);
      String[] nestedFields = fieldName.split(DOC_PATH_SEPARATOR);
      for (int i = 0; i < nestedFields.length - 1; i++) {
        fieldPrefix.append("->" + "'").append(nestedFields[i]).append("'");
      }
      fieldPrefix.append("->>").append("'").append(nestedFields[nestedFields.length - 1]).append("'");
    }
    return fieldPrefix.toString();
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
    String deleteSQL = String.format("DELETE FROM %s WHERE %s = ?", collectionName, ID);
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
    String deleteSubDocSQL = String.format("UPDATE %s SET %s=%s #- ?::text[] WHERE %s=?",
      collectionName, DOCUMENT, DOCUMENT, ID);
    String jsonSubDocPath = getJsonSubDocPath(subDocPath);
    try {
      
      PreparedStatement preparedStatement = client.prepareStatement(deleteSubDocSQL, Statement.RETURN_GENERATED_KEYS);
      preparedStatement.setString(1, jsonSubDocPath);
      preparedStatement.setString(2, key.toString());
      int resultSet = preparedStatement.executeUpdate();
      
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: " + resultSet);
      }
      
      return true;
    } catch (SQLException e) {
      LOGGER.error("SQLException updating sub document. key: {} subDocPath: {}", key, subDocPath, e);
    }
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
    long count = -1;
    try {
      PreparedStatement preparedStatement = client.prepareStatement(countSQL);
      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) count = resultSet.getLong(1);
    } catch (SQLException e) {
      LOGGER.error("SQLException counting all documents.", e);
    }
    return count;
  }
  
  @Override
  public long total(Query query) {
    StringBuilder totalSQLBuilder = new StringBuilder("SELECT COUNT(*) FROM ").append(collectionName);
    long count = -1;
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
    try {
      PreparedStatement preparedStatement = client.prepareStatement(getUpsertSQL(), Statement.RETURN_GENERATED_KEYS);
      for (Map.Entry<Key, Document> entry : documents.entrySet()) {
        
        Key key = entry.getKey();
        String jsonString = prepareUpsertDocument(key, entry.getValue());
        
        preparedStatement.setString(1, key.toString());
        preparedStatement.setString(2, jsonString);
        preparedStatement.setString(3, jsonString);
        
        preparedStatement.addBatch();
      }
      
      if (preparedStatement == null) return false;
      
      int[] updateCounts = preparedStatement.executeBatch();
      
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: " + Arrays.toString(updateCounts));
      }
      
      return true;
    } catch (BatchUpdateException e) {
      LOGGER.error("BatchUpdateException bulk inserting documents.", e);
    } catch (SQLException e) {
      LOGGER.error("SQLException bulk inserting documents. SQLState: {} Error Code:{}", e.getSQLState(), e.getErrorCode(), e);
    } catch (IOException e) {
      LOGGER.error("SQLException bulk inserting documents. documents: {}", documents, e);
    }
    
    return false;
  }
  
  private String prepareUpsertDocument(Key key, Document document) throws IOException {
    String jsonString = document.toJson();
    
    ObjectNode jsonNode = (ObjectNode) MAPPER.readTree(jsonString);
    jsonNode.put(DOCUMENT_ID, key.toString());
    
    return MAPPER.writeValueAsString(jsonNode);
  }
  
  private String getUpsertSQL() {
    return String.format("INSERT INTO %s (%s,%s) VALUES( ?, ? :: jsonb) ON CONFLICT(%s) DO UPDATE SET %s = ?::jsonb ",
      collectionName, ID, DOCUMENT, ID, DOCUMENT);
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
        String documentString = resultSet.getString(DOCUMENT);
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

