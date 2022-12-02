package org.hypertrace.core.documentstore.model.subdoc;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum UpdateOperator {
  SET,
  UNSET,
  REMOVE, // Remove all occurrences of a value from a list
  ADD, // Add to an existing set/list ensuring uniqueness,
  APPEND, // Simply append to the existing list,
  ;

  /**
   * "jsonb_set()" "#-" Unset sub doc path jsondata = jsondata || '["newString"]'::jsonb "-" Remove
   * from array select jsonb_agg(distinct e) from jsonb_array_elements('[1,2,2,3]'::jsonb) as t(e)
   */
}
