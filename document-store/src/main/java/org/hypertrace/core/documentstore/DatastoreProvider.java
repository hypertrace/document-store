package org.hypertrace.core.documentstore;

import com.typesafe.config.Config;
import org.hypertrace.core.documentstore.mongo.MongoDatastore;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DatastoreProvider {

  private static Map<String, Class<? extends Datastore>> registry = new ConcurrentHashMap<>();

  static {
    DatastoreProvider.register("Mongo", MongoDatastore.class);
    DatastoreProvider.register("Postgres", PostgresDatastore.class);
  }

  /**
   * Creates a DocDatastore, currently it creates a new client/connection on every invocation. We
   * might add pooling later
   * @return {@link Datastore}
   */
  public static Datastore getDatastore(String type, Config config) {

    Class<? extends Datastore> clazz = registry.get(type.toLowerCase());
    try {
      Constructor<? extends Datastore> constructor = clazz.getConstructor();
      Datastore instance = constructor.newInstance();
      instance.init(config);
      return instance;
    } catch (Exception e) {
      throw new IllegalArgumentException("Exception creating DocDatastore", e);
    }

  }

  /**
   * Register various possible implementations. Expects a constructor with no-args and an init
   * method that takes in ParamsMap
   */
  public static void register(String type, Class<? extends Datastore> clazz) {
    registry.put(type.toLowerCase(), clazz);
  }

}
