package org.hypertrace.core.documentstore.commons;

import static org.hypertrace.core.documentstore.model.subdoc.SubDocument.PATH_SEPARATOR;

import java.io.IOException;
import java.util.Collection;
import org.hypertrace.core.documentstore.model.subdoc.SubDocument;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;

public class CommonUpdateValidator implements UpdateValidator {
  public void validate(final Collection<SubDocumentUpdate> updates) throws IOException {
    validateAtLeastOneUpdateIsPresent(updates);
    validateUpdateNotPerformedInSameHierarchy(updates);
  }

  private void validateUpdateNotPerformedInSameHierarchy(Collection<SubDocumentUpdate> updates)
      throws IOException {
    final String[] sortedPaths =
        updates.stream()
            .map(SubDocumentUpdate::getSubDocument)
            .map(SubDocument::getPath)
            .sorted()
            .toArray(String[]::new);
    for (int i = 1; i < sortedPaths.length; i++) {
      if (arePartOfSameHierarchy(sortedPaths[i - 1], sortedPaths[i])) {
        throw new IOException(
            String.format(
                "Cannot update paths in the same hierarchy (%s and %s)",
                sortedPaths[i - 1], sortedPaths[i]));
      }
    }
  }

  private boolean arePartOfSameHierarchy(final String first, final String second) {
    // If "a.b.c" (second) starts with "a." (first), then they are part of the same hierarchy
    return first.equals(second) || second.startsWith(first + PATH_SEPARATOR);
  }

  private void validateAtLeastOneUpdateIsPresent(final Collection<SubDocumentUpdate> updates)
      throws IOException {
    if (updates.isEmpty()) {
      throw new IOException("At least one update must be supplied");
    }
  }
}
