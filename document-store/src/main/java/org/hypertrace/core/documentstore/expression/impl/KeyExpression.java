package org.hypertrace.core.documentstore.expression.impl;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.commons.DocStoreConstants.IMPLICIT_ID;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class KeyExpression implements FilterTypeExpression {
  List<Key> keys;

  public static KeyExpression of(final Key firstKey, final Key... otherKeys) {
    Preconditions.checkArgument(firstKey != null, "key is null");
    Preconditions.checkArgument(Arrays.stream(otherKeys).allMatch(Objects::nonNull), "key is null");
    return new KeyExpression(
        Stream.concat(Stream.of(firstKey), Arrays.stream(otherKeys)).collect(toUnmodifiableList()));
  }

  public static KeyExpression of(final List<Key> keys) {
    Preconditions.checkArgument(keys.stream().allMatch(Objects::nonNull), "key is null");
    return new KeyExpression(keys);
  }

  public static KeyExpression from(final String firstKeyString, final String... otherKeyStrings) {
    return new KeyExpression(
        Stream.concat(Stream.of(firstKeyString), Arrays.stream(otherKeyStrings))
            .map(Key::from)
            .collect(toUnmodifiableList()));
  }

  public static KeyExpression from(final List<String> keyStrings) {
    return new KeyExpression(keyStrings.stream().map(Key::from).collect(toUnmodifiableList()));
  }

  @Override
  public <T> T accept(final FilterTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    final String combined =
        keys.stream().map(Key::toString).map(this::wrapQuotes).collect(joining(", "));
    return IMPLICIT_ID + " IN (" + combined + ")";
  }

  private String wrapQuotes(final String original) {
    return StringUtils.wrap(original, "'");
  }
}
