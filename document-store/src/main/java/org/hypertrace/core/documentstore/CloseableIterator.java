package org.hypertrace.core.documentstore;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;

public interface CloseableIterator<E> extends Iterator<E>, Closeable {
  static <E> CloseableIterator<E> emptyIterator() {
    return new CloseableIterator<>() {
      @Override
      public void close() {
        // Do nothing
      }

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public E next() {
        throw new NoSuchElementException();
      }
    };
  }
}
