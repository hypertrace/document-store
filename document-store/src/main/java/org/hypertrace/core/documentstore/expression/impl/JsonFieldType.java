package org.hypertrace.core.documentstore.expression.impl;

/** Represents the type of JSON fields in flat collections */
public enum JsonFieldType {
  STRING,
  NUMBER,
  BOOLEAN,
  STRING_ARRAY,
  NUMBER_ARRAY,
  BOOLEAN_ARRAY,
  OBJECT_ARRAY,
  OBJECT,
  UNSPECIFIED
}
