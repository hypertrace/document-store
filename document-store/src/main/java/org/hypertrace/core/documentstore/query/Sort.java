package org.hypertrace.core.documentstore.query;

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
          sortingSpecs.stream().noneMatch(Objects::isNull), "One ore more sortingSpecs is null");
      return new Sort(sortingSpecs);
    }
  }
}
