package org.hypertrace.core.documentstore.model.config.mongo;

import java.util.Optional;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials;

@Value
@SuperBuilder
@Accessors(fluent = true)
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class MongoConnectionCredentials extends ConnectionCredentials {
  @Nullable @Builder.Default String authDatabase = null;

  @SuppressWarnings("ConstantConditions")
  public Optional<String> authDatabase() {
    return Optional.ofNullable(authDatabase);
  }
}
