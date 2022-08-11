package org.hypertrace.core.documentstore;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InsightDto {
  public static final String ID_FIELD = "id";
  public static final String TYPE_FIELD = "type";
  public static final String ATTRIBUTES_FIELD = "attributes";
  public static final String IDENTIFYING_ATTRIBUTES_FIELD = "identifyingAttributes";
  public static final String TENANT_ID_FIELD = "tenantId";

  @JsonProperty(value = ID_FIELD)
  private String id;

  @JsonProperty(value = TYPE_FIELD)
  private String type;

  @JsonProperty(value = ATTRIBUTES_FIELD)
  private Map<String, Object> attributes;

  @JsonProperty(value = IDENTIFYING_ATTRIBUTES_FIELD)
  private Map<String, String> identifyingAttributes;

  @JsonProperty(value = TENANT_ID_FIELD)
  private String tenantId;
}
