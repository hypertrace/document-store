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
public class Sort {
  @Singular List<SortingSpec> sortingSpecs;

  public static class SortBuilder {
    public Sort build() {
      Preconditions.checkArgument(!sortingSpecs.isEmpty(), "sortingSpecs is empty");
      Preconditions.checkArgument(
          sortingSpecs.stream().noneMatch(Objects::isNull), "One or more sortingSpecs is null");
      return new Sort(sortingSpecs);
    }
  }

  @Override
  public String toString() {
    return sortingSpecs.stream().map(String::valueOf).collect(joining(", "));
  }
}
