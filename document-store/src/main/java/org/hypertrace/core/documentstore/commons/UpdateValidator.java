package org.hypertrace.core.documentstore.commons;

import java.io.IOException;
import java.util.Collection;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;

public interface UpdateValidator {
  void validate(final Collection<SubDocumentUpdate> updates) throws IOException;
}
