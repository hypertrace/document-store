package org.hypertrace.core.documentstore.model.subdoc;

import static lombok.AccessLevel.PRIVATE;
import static org.hypertrace.core.documentstore.commons.DocStoreConstants.CREATED_TIME;
import static org.hypertrace.core.documentstore.commons.DocStoreConstants.LAST_UPDATED_TIME;

import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor(access = PRIVATE)
public class SubDocument {
  private static final Set<String> IMPLICIT_PATHS = Set.of(CREATED_TIME, LAST_UPDATED_TIME);

  String path;

  public static SubDocument implicitCreatedTime() {
    return new SubDocument(CREATED_TIME);
  }

  public static SubDocument implicitUpdatedTime() {
    return new SubDocument(LAST_UPDATED_TIME);
  }

  public static class SubDocumentBuilder {
    public SubDocument build() {
      validatePath();
      return new SubDocument(path);
    }

    private void validatePath() {
      if (IMPLICIT_PATHS.contains(path)) {
        throw new IllegalArgumentException(
            String.format("%s is maintained implicitly. Please use a different path.", path));
      }
    }
  }
}
