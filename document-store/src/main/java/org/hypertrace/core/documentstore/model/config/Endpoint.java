package org.hypertrace.core.documentstore.model.config;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder(toBuilder = true)
@Accessors(fluent = true)
public class Endpoint {
  private static final String DEFAULT_HOST = "localhost";

  @Default @NonNull String host = DEFAULT_HOST;
  @Nullable @Nonnegative Integer port;
}
