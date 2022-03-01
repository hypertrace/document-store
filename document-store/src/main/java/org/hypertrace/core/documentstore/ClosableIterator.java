package org.hypertrace.core.documentstore;

import java.io.Closeable;
import java.util.Iterator;

public interface ClosableIterator<E> extends Iterator<E>, Closeable {}
