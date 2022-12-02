package org.hypertrace.core.documentstore.commons;

import static org.hypertrace.core.documentstore.model.subdoc.SubDocument.PATH_SEPARATOR;

import java.io.IOException;
import java.util.Collection;
import org.hypertrace.core.documentstore.model.subdoc.SubDocument;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;

public class CommonUpdateValidator implements UpdateValidator {
  public void validate(final Collection<SubDocumentUpdate> updates) throws IOException {
    if (updates.isEmpty()) {
      throw new IOException("At least one update must be supplied");
    }

    final String[] sortedPaths =
        updates.stream()
            .map(SubDocumentUpdate::getSubDocument)
            .map(SubDocument::getPath)
            .sorted()
            .toArray(String[]::new);
    for (int i = 1; i < sortedPaths.length; i++) {
      if (sortedPaths[i - 1].equals(sortedPaths[i])
          || sortedPaths[i].startsWith(sortedPaths[i - 1] + PATH_SEPARATOR)) {
        throw new IOException(
            String.format(
                "Cannot update paths in the same hierarchy (%s and %s)",
                sortedPaths[i], sortedPaths[i - 1]));
      }
    }
  }
}
