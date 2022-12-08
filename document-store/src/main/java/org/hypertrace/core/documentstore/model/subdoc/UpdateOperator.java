package org.hypertrace.core.documentstore.model.subdoc;

public enum UpdateOperator {
  SET,
  UNSET,
  REMOVE_ALL_FROM_LIST,
  ADD_TO_LIST_IF_ABSENT,
  APPEND_TO_LIST,
}
