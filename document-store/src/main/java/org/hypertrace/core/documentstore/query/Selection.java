package org.hypertrace.core.documentstore.query;

import static java.util.stream.Collectors.joining;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Selection {
  @Singular List<SelectionSpec> selectionSpecs;

  public static class SelectionBuilder {
    public Selection build() {
      Preconditions.checkArgument(!selectionSpecs.isEmpty(), "selectionSpecs is empty");
      Preconditions.checkArgument(
          selectionSpecs.stream().noneMatch(Objects::isNull), "One or more selectionSpecs is null");
      return new Selection(selectionSpecs);
    }
  }

  @Override
  public String toString() {
    return selectionSpecs.stream().map(String::valueOf).collect(joining(", "));
  }
}
