package org.hypertrace.core.documentstore.mongo;

import com.mongodb.ServerAddress;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MongoDatastoreTest {

  @Test
  public void testInitUsingHostPort() {
    String host = "localhost";
    int port = 27017;
    MongoDatastore datastore = new MongoDatastore();
    Properties properties = new Properties();
    properties.setProperty("host", host);
    properties.setProperty("port", String.valueOf(port));
    Config config = ConfigFactory.parseProperties(properties);
    datastore.init(config);
    List<ServerAddress> servers = datastore.getMongoClient().getAllAddress();
    Assertions.assertEquals(servers.size(), 1);
    Assertions.assertEquals(servers.get(0).getHost(), host);
    Assertions.assertEquals(servers.get(0).getPort(), port);
  }

  @Test
  public void testInitUsingUrl() {
    MongoDatastore datastore = new MongoDatastore();
    Properties properties = new Properties();
    properties.setProperty("host", "localhost");
    properties.setProperty("port", "27017");
    properties.setProperty("url", "mongodb://mongo-0:27017,mongo-1:27017/?replicaSet=rs0");
    Config config = ConfigFactory.parseProperties(properties);
    datastore.init(config);
    List<ServerAddress> servers = datastore.getMongoClient().getAllAddress();
    Assertions.assertEquals(servers.size(), 2);
  }
}
