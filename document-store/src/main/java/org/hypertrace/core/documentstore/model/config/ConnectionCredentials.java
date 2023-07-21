package org.hypertrace.core.documentstore.model.config;

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder(toBuilder = true)
@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectionCredentials {
  @Default @Nonnull String username = "";

  @Default @Nonnull @ToString.Exclude String password = "";

  @Nullable @Builder.Default String authDatabase = null;

  @SuppressWarnings("ConstantConditions")
  public Optional<String> authDatabase() {
    return Optional.ofNullable(authDatabase);
  }
}
