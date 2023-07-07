package org.hypertrace.core.documentstore.model.config.postgres;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials;

@Value
@SuperBuilder
@Accessors(fluent = true)
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PostgresConnectionCredentials extends ConnectionCredentials {

}
