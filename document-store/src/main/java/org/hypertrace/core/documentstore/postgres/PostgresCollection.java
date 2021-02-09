package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.OrderBy;
import org.hypertrace.core.documentstore.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides {@link Collection} implementation on Postgres using jsonb format
 */
public class PostgresCollection implements Collection {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCollection.class);
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
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String DOC_PATH_SEPARATOR = "\\.";
  private static String QUESTION_MARK = "?";


  private final Connection client;
  private final String collectionName;

  public PostgresCollection(Connection client, String collectionName) {
    this.client = client;
    this.collectionName = collectionName;
  }

  @Override
  public boolean upsert(Key key, Document document) throws IOException {
    try (PreparedStatement preparedStatement = client
            .prepareStatement(getUpsertSQL(), Statement.RETURN_GENERATED_KEYS)) {
      String jsonString = prepareUpsertDocument(key, document);
      preparedStatement.setString(1, key.toString());
      preparedStatement.setString(2, jsonString);
      preparedStatement.setString(3, jsonString);
      int result = preparedStatement.executeUpdate();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", result);
      }
      return result >= 0;
    } catch (SQLException e) {
      LOGGER.error("SQLException inserting document. key: {} content:{}", key, document, e);
      throw new IOException(e);
    }
  }

  /**
   * For Postgres upsertAndReturn functionality is not supported directly.
   * So using direct upsert and then return document.
   */
  @Override
  public Document upsertAndReturn(Key key, Document document) throws IOException {
    try {
      boolean upsert = upsert(key, document);
      return upsert ? document : null;
    } catch (IOException e) {
      LOGGER.error("SQLException inserting document. key: {} content:{}", key, document, e);
      throw e;
    }
  }

  /**
   * Update the sub document based on subDocPath based on longest key match.
   *
   * As an e.g
   * {
   *   "first" : "name",
   *   "last" : "lastname"
   *   "address" : {
   *     "street" : "long street"
   *     "pin" : "00000"
   *   }
   * }
   *
   * Following subDocPath will match,
   * first
   * address.street
   * address
   * address.pin
   *
   * Following creates new sub-document for matching subDocPath
   * address.street.longitude (here address.street matches)
   *
   * Following subDocPath will not match any sub document,
   * address.street.longitude.degree
   *
   */
  @Override
  public boolean updateSubDoc(Key key, String subDocPath, Document subDocument) {
    String updateSubDocSQL = String
        .format("UPDATE %s SET %s=jsonb_set(%s, ?::text[], ?::jsonb) WHERE %s=?",
            collectionName, DOCUMENT, DOCUMENT, ID);
    String jsonSubDocPath = getJsonSubDocPath(subDocPath);
    String jsonString = subDocument.toJson();

    try (PreparedStatement preparedStatement = client
            .prepareStatement(updateSubDocSQL, Statement.RETURN_GENERATED_KEYS)) {
      preparedStatement.setString(1, jsonSubDocPath);
      preparedStatement.setString(2, jsonString);
      preparedStatement.setString(3, key.toString());
      int resultSet = preparedStatement.executeUpdate();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", resultSet);
      }

      return true;
    } catch (SQLException e) {
      LOGGER.error("SQLException updating sub document. key: {} subDocPath: {} content:{}", key,
          subDocPath, subDocument, e);
    }
    return false;
  }

  @Override
  public Iterator<Document> search(Query query) {
    String filters = null;
    StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM ")
        .append(collectionName);
    Params.Builder paramsBuilder = Params.newBuilder();

    // If there is a filter in the query, parse it fully.
    if (query.getFilter() != null) {
      filters = parseFilter(query.getFilter(), paramsBuilder);
    }

    LOGGER.debug(
        "Sending query to PostgresSQL: {} : {}",
        collectionName,
        filters);

    if (filters != null) {
      sqlBuilder
          .append(" WHERE ").append(filters);
    }

    if (!query.getOrderBys().isEmpty()) {
      String orderBySQL = parseOrderByQuery(query.getOrderBys());
      sqlBuilder.append(" ORDER BY ").append(orderBySQL);
    }

    Integer limit = query.getLimit();
    if (limit != null && limit >= 0) {
      sqlBuilder.append(" LIMIT ").append(limit);
    }

    Integer offset = query.getOffset();
    if (offset != null && offset >= 0) {
      sqlBuilder.append(" OFFSET ").append(offset);
    }

    try {
      PreparedStatement preparedStatement = buildPreparedStatement(sqlBuilder.toString(), paramsBuilder.build());
      ResultSet resultSet = preparedStatement.executeQuery();
      return new PostgresResultIterator(resultSet);
    } catch (SQLException e) {
      LOGGER.error("SQLException querying documents. query: {}", query, e);
    }

    return Collections.emptyIterator();
  }

  @VisibleForTesting
  protected String parseFilter(Filter filter, Params.Builder paramsBuilder) {
    if (filter.isComposite()) {
      return parseCompositeFilter(filter, paramsBuilder);
    } else {
      return parseNonCompositeFilter(filter, paramsBuilder);
    }
  }

  @VisibleForTesting
  protected String parseNonCompositeFilter(Filter filter, Params.Builder paramsBuilder) {
    Filter.Op op = filter.getOp();
    Object value = filter.getValue();
    String fieldName = filter.getFieldName();
    String fullFieldName = prepareCast(getFieldPrefix(fieldName), value);
    StringBuilder filterString = new StringBuilder(fullFieldName);
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
            .map(val -> {
              paramsBuilder.addObjectParam(val);
              return QUESTION_MARK;
            })
            .collect(Collectors.joining(", "));
        return filterString.append("(").append(collect).append(")").toString();
      case CONTAINS:
        // TODO: Matches condition inside an array of documents
      case EXISTS:
        // TODO: Checks if key exists
      case NOT_EXISTS:
        // TODO: Checks if key does not exist
      case NEQ:
      default:
        throw new UnsupportedOperationException(
            String.format("Query operation:%s not supported", op));
    }
    String filters = filterString.append(QUESTION_MARK).toString();
    paramsBuilder.addObjectParam(value);
    return filters;
  }

  @VisibleForTesting
  protected String parseCompositeFilter(Filter filter, Params.Builder paramsBuilder) {
    Filter.Op op = filter.getOp();
    switch (op) {
      case OR: {
        String childList =
            Arrays.stream(filter.getChildFilters())
                .map(childFilter -> parseFilter(childFilter, paramsBuilder))
                .filter(str -> !StringUtils.isEmpty(str))
                .map(str -> "(" + str + ")")
                .collect(Collectors.joining(" OR "));
        return !childList.isEmpty() ? childList : null;
      }
      case AND: {
        String childList =
            Arrays.stream(filter.getChildFilters())
                .map(childFilter -> parseFilter(childFilter, paramsBuilder))
                .filter(str -> !StringUtils.isEmpty(str))
                .map(str -> "(" + str + ")")
                .collect(Collectors.joining(" AND "));
        return !childList.isEmpty() ? childList : null;
      }
      default:
        throw new UnsupportedOperationException(
            String.format("Query operation:%s not supported", op));
    }
  }

  @VisibleForTesting
  protected PreparedStatement buildPreparedStatement(String sqlQuery, Params params) throws SQLException, RuntimeException {
    PreparedStatement preparedStatement = client.prepareStatement(sqlQuery);
    params.getObjectParams().forEach((k, v) -> {
      try {
        if (isValidType(v)) {
          preparedStatement.setObject(k, v);
        } else {
          throw new UnsupportedOperationException("Un-supported object types in filter");
        }
      } catch (SQLException e) {
        LOGGER.error("SQLException setting Param. key: {}, value: {}", k, v);
      }
    });
    return preparedStatement;
  }

  private boolean isValidType(Object v) {
    Set<Class<?>> validClassez = new HashSet<>() {{
      add(Double.class);
      add(Float.class);
      add(Integer.class);
      add(Long.class);
      add(String.class);
      add(Boolean.class);
      add(Number.class);
    }};
    return validClassez.contains(v.getClass());
  }

  @VisibleForTesting
  private String getJsonSubDocPath(String subDocPath) {
    return "{" + subDocPath.replaceAll(DOC_PATH_SEPARATOR, ",") + "}";
  }

  /**
   * Add field prefix for searching into json document based on postgres syntax, handles nested
   * keys. Note: It doesn't handle array elements in json document. e.g SELECT * FROM TABLE where
   * document ->> 'first' = 'name' and document -> 'address' ->> 'pin' = "00000"
   */
  private String getFieldPrefix(String fieldName) {
    StringBuilder fieldPrefix = new StringBuilder(fieldName);
    if (!OUTER_COLUMNS.contains(fieldName)) {
      fieldPrefix = new StringBuilder(DOCUMENT);
      String[] nestedFields = fieldName.split(DOC_PATH_SEPARATOR);
      for (int i = 0; i < nestedFields.length - 1; i++) {
        fieldPrefix.append("->" + "'").append(nestedFields[i]).append("'");
      }
      fieldPrefix.append("->>").append("'").append(nestedFields[nestedFields.length - 1])
          .append("'");
    }
    return fieldPrefix.toString();
  }

  private String parseOrderByQuery(List<OrderBy> orderBys) {
    return orderBys
        .stream()
        .map(orderBy -> getFieldPrefix(orderBy.getField()) + " " + (orderBy.isAsc() ? "ASC" : "DESC"))
        .filter(str -> !StringUtils.isEmpty(str))
        .collect(Collectors.joining(" , "));
  }

  @Override
  public boolean delete(Key key) {
    String deleteSQL = String.format("DELETE FROM %s WHERE %s = ?", collectionName, ID);
    try (PreparedStatement preparedStatement = client.prepareStatement(deleteSQL)) {
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

    try (PreparedStatement preparedStatement = client
        .prepareStatement(deleteSubDocSQL, Statement.RETURN_GENERATED_KEYS)) {
      preparedStatement.setString(1, jsonSubDocPath);
      preparedStatement.setString(2, key.toString());
      int resultSet = preparedStatement.executeUpdate();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", resultSet);
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
    try (PreparedStatement preparedStatement = client.prepareStatement(deleteSQL)) {
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
    try (PreparedStatement preparedStatement = client.prepareStatement(countSQL)) {
      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        count = resultSet.getLong(1);
      }
    } catch (SQLException e) {
      LOGGER.error("SQLException counting all documents.", e);
    }
    return count;
  }

  @Override
  public long total(Query query) {
    StringBuilder totalSQLBuilder = new StringBuilder("SELECT COUNT(*) FROM ")
        .append(collectionName);
    Params.Builder paramsBuilder = Params.newBuilder();

    long count = -1;
    // on any in-correct filter input, it will return total without filtering
    if (query.getFilter() != null) {
      String parsedQuery = parseFilter(query.getFilter(), paramsBuilder);
      if (parsedQuery != null) {
        totalSQLBuilder.append(" WHERE ").append(parsedQuery);
      }
    }

    try (PreparedStatement preparedStatement = buildPreparedStatement(totalSQLBuilder.toString(), paramsBuilder.build())) {
      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        count = resultSet.getLong(1);
      }
    } catch (SQLException e) {
      LOGGER.error("SQLException querying documents. query: {}", query, e);
    }
    return count;
  }

  @Override
  public boolean bulkUpsert(Map<Key, Document> documents) {
    try {
      int[] updateCounts = bulkUpsertImpl(documents);

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", Arrays.toString(updateCounts));
      }

      return true;
    } catch (BatchUpdateException e) {
      LOGGER.error("BatchUpdateException bulk inserting documents.", e);
    } catch (SQLException e) {
      LOGGER.error("SQLException bulk inserting documents. SQLState: {} Error Code:{}",
          e.getSQLState(), e.getErrorCode(), e);
    } catch (IOException e) {
      LOGGER.error("SQLException bulk inserting documents. documents: {}", documents, e);
    }

    return false;
  }

  private int[] bulkUpsertImpl(Map<Key, Document> documents) throws SQLException, IOException {
    try (PreparedStatement preparedStatement = client
        .prepareStatement(getUpsertSQL(), Statement.RETURN_GENERATED_KEYS)) {
      for (Map.Entry<Key, Document> entry : documents.entrySet()) {

        Key key = entry.getKey();
        String jsonString = prepareUpsertDocument(key, entry.getValue());

        preparedStatement.setString(1, key.toString());
        preparedStatement.setString(2, jsonString);
        preparedStatement.setString(3, jsonString);

        preparedStatement.addBatch();
      }

      return preparedStatement.executeBatch();
    }
  }

  @Override
  public Iterator<Document> bulkUpsertAndReturnOlderDocuments(Map<Key, Document> documents) throws IOException {
    String query = null;
    try {
      String collect = documents.keySet().stream()
          .map(val -> "'" + val.toString() + "'")
          .collect(Collectors.joining(", "));

      String space = " ";
      query = new StringBuilder("SELECT * FROM")
          .append(space).append(collectionName)
          .append(" WHERE ").append(ID).append(" IN ")
          .append("(").append(collect).append(")").toString();

      PreparedStatement preparedStatement = client.prepareStatement(query);
      ResultSet resultSet = preparedStatement.executeQuery();

      // Now go ahead and bulk upsert the documents.
      int[] updateCounts = bulkUpsertImpl(documents);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", Arrays.toString(updateCounts));
      }

      return new PostgresResultIterator(resultSet);
    } catch (IOException e) {
      LOGGER.error("SQLException bulk inserting documents. documents: {}", documents, e);
    } catch (SQLException e) {
      LOGGER.error("SQLException querying documents. query: {}", query, e);
    }

    throw new IOException("Could not bulk upsert the documents.");
  }

  private String prepareUpsertDocument(Key key, Document document) throws IOException {
    String jsonString = document.toJson();

    ObjectNode jsonNode = (ObjectNode) MAPPER.readTree(jsonString);
    jsonNode.put(DOCUMENT_ID, key.toString());

    return MAPPER.writeValueAsString(jsonNode);
  }

  private String prepareCast(String field, Object value) {
    String fmt = "CAST (%s AS %s)";

    // handle the case if the value type is collection for filter operator - `IN`
    // Currently, for `IN` operator, we are considering List collection, and it is fair
    // assumption that all its value of the same types. Based on that and for consistency
    // we will use CAST ( <field name> as <type> ) for all non string operator.
    // Ref : https://github.com/hypertrace/document-store/pull/30#discussion_r571782575
    
    if (value instanceof List<?> && ((List<Object>) value).size() > 0) {
      List<Object> listValue = (List<Object>) value;
      value = listValue.get(0);
    }

    if (value instanceof Number) {
      return String.format(fmt, field, "NUMERIC");
    } else if (value instanceof Boolean) {
      return String.format(fmt, field, "BOOLEAN");
    } else /* default is string */ {
      return field;
    }
  }

  private String getUpsertSQL() {
    return String.format(
        "INSERT INTO %s (%s,%s) VALUES( ?, ? :: jsonb) ON CONFLICT(%s) DO UPDATE SET %s = ?::jsonb ",
        collectionName, ID, DOCUMENT, ID, DOCUMENT);
  }

  @Override
  public void drop() {
    String dropTableSQL = String.format("DROP TABLE IF EXISTS %s", collectionName);
    try (PreparedStatement preparedStatement = client.prepareStatement(dropTableSQL)) {
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Exception deleting table name: {}", collectionName);
    }
  }

  static class PostgresResultIterator implements Iterator {

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
        jsonNode.remove(DOCUMENT_ID);
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

    private String prepareNumericBlock(String fieldName, Object value) {
      if (value instanceof Number) {
        String fmt = "case jsonb_typeof(%s)\n"
            + "WHEN ‘number’ THEN (%s)::numeric > ?\n"
            + "end";
      } else if (value instanceof Boolean) {
        String fmtBoolean = "case jsonb_typeof(<field>)\n"
            + "WHEN 'boolean'  THEN (<field>)::boolean  > ?\n"
            + "end";
      }
      return null;
    }
  }
}

