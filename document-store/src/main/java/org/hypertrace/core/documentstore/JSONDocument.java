package org.hypertrace.core.documentstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Objects;

public class JSONDocument implements Document {

    private static ObjectMapper mapper = new ObjectMapper();
    private JsonNode node;

    public JSONDocument(String json) throws IOException {
        node = mapper.readTree(json);
    }

    public JSONDocument(Object object) throws IOException {
        node = mapper.readTree(mapper.writeValueAsString(object));
    }

    public JSONDocument(JsonNode node) {
        this.node = node;
    }

    @Override
    public String toJson() {
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    public static JSONDocument errorDocument(String message) {
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put("errorMessage", message);
        return new JSONDocument(objectNode);
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        JSONDocument other = (JSONDocument) obj;
        return Objects.equals(node, other.node);
    }
}
