package org.hypertrace.core.documentstore.model.config;

import java.util.Optional;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder(toBuilder = true)
@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectionCredentials {
  @Default @NonNull String username = "";

  @Default @NonNull @ToString.Exclude String password = "";

  @Default @Nullable String authDatabase = null;

  @SuppressWarnings("ConstantConditions")
  public Optional<String> authDatabase() {
    return Optional.ofNullable(authDatabase);
  }
}
