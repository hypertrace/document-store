package org.hypertrace.core.documentstore.postgres;

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
public class PostgresTableIdentifier {

  // If not specified, no schema will be used. By default, postgres treats this as the "public"
  // schema.
  @Nullable private final String schema;

  @Getter @Nonnull private final String quotedTable;

  public Optional<String> getSchema() {
    return Optional.ofNullable(this.schema);
  }

  PostgresTableIdentifier(String tableName) {
    this(null, tableName);
  }

  PostgresTableIdentifier(@Nullable String schema, @Nonnull String tableName) {
    this.schema = schema;
    this.quotedTable = "\"" + tableName + "\"";
  }

  public static PostgresTableIdentifier parse(String tableString) {
    String[] tableComponents = tableString.split("\\.", 2);
    if (tableComponents.length == 2) {
      return new PostgresTableIdentifier(tableComponents[0], tableComponents[1]);
    }
    return new PostgresTableIdentifier(tableComponents[0]);
  }

  @Override
  public String toString() {
    return this.getSchema()
        .map(schema -> schema + "." + this.getQuotedTable())
        .orElseGet(this::getQuotedTable);
  }
}
