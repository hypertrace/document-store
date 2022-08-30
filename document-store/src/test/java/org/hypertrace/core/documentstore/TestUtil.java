package org.hypertrace.core.documentstore;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;

public class TestUtil {
  public static String readFileFromResource(final String filePath)
      throws IOException, URISyntaxException {
    final ClassLoader classLoader = TestUtil.class.getClassLoader();
    final URL resource = classLoader.getResource(filePath);

    if (resource == null) {
      throw new IllegalArgumentException("File not found! " + filePath);
    }

    return String.join("\n", Files.readAllLines(new File(resource.toURI()).toPath()));
  }
}
