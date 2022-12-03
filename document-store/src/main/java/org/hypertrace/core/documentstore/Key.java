package org.hypertrace.core.documentstore;

import com.google.common.base.Preconditions;

public interface Key {
  String toString();

  static Key from(final String string) {
    Preconditions.checkNotNull(string);

    return new Key() {
      @Override
      public String toString() {
        return string;
      }
    };
  }
}
