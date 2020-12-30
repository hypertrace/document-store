package org.hypertrace.core.documentstore;

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JSONDocumentTest {

    @Test
    public void testJSONDocument() throws Exception {
        Map<String, String> data = Map.of("key1", "value1", "key2", "value2");
        JSONDocument document1 = new JSONDocument(data);
        JSONDocument document2 = new JSONDocument(document1.toJson());
        Assertions.assertEquals(document1, document2);
        Assertions.assertEquals(document1.toJson(), document2.toJson());
    }
}
