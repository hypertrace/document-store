package org.hypertrace.core.documentstore.postgres;

import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.Query;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class PostgresCollection implements Collection {
  @Override
  public boolean upsert(Key key, Document document) throws IOException {
    return false;
  }
  
  @Override
  public boolean updateSubDoc(Key key, String subDocPath, Document subDocument) {
    return false;
  }
  
  @Override
  public Iterator<Document> search(Query query) {
    return null;
  }
  
  @Override
  public boolean delete(Key key) {
    return false;
  }
  
  @Override
  public boolean deleteSubDoc(Key key, String subDocPath) {
    return false;
  }
  
  @Override
  public boolean deleteAll() {
    return false;
  }
  
  @Override
  public long count() {
    return 0;
  }
  
  @Override
  public long total(Query query) {
    return 0;
  }
  
  @Override
  public boolean bulkUpsert(Map<Key, Document> documents) {
    return false;
  }
  
  @Override
  public void drop() {
  
  }
}
