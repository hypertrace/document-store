package org.hypertrace.core.documentstore.model.subdoc;

import static lombok.AccessLevel.PRIVATE;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.SET;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.UNSET;

import com.google.common.base.Preconditions;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Value
@Builder
@AllArgsConstructor(access = PRIVATE)
public class SubDocumentUpdate {
  @Nonnull SubDocument subDocument;
  @Nonnull UpdateOperator operator;
  @Nonnull SubDocumentValue subDocumentValue;

  public static SubDocumentUpdate of(final String subDocumentPath, final String value) {
    return of(subDocumentPath, SubDocumentValue.of(value));
  }

  public static SubDocumentUpdate of(final String subDocumentPath, final Number value) {
    return of(subDocumentPath, SubDocumentValue.of(value));
  }

  public static SubDocumentUpdate of(final String subDocumentPath, final Boolean value) {
    return of(subDocumentPath, SubDocumentValue.of(value));
  }

  public static SubDocumentUpdate of(final String subDocumentPath, final String[] value) {
    return of(subDocumentPath, SubDocumentValue.of(value));
  }

  public static SubDocumentUpdate of(final String subDocumentPath, final Number[] value) {
    return of(subDocumentPath, SubDocumentValue.of(value));
  }

  public static SubDocumentUpdate of(final String subDocumentPath, final Boolean[] value) {
    return of(subDocumentPath, SubDocumentValue.of(value));
  }

  public static SubDocumentUpdate of(final String subDocumentPath, final SubDocumentValue value) {
    return new SubDocumentUpdate(SubDocument.builder().path(subDocumentPath).build(), SET, value);
  }

  public static SubDocumentUpdate of(final SubDocument subDocument, final String value) {
    return of(subDocument, SubDocumentValue.of(value));
  }

  public static SubDocumentUpdate of(final SubDocument subDocument, final Number value) {
    return of(subDocument, SubDocumentValue.of(value));
  }

  public static SubDocumentUpdate of(final SubDocument subDocument, final Boolean value) {
    return of(subDocument, SubDocumentValue.of(value));
  }

  public static SubDocumentUpdate of(final SubDocument subDocument, final String[] value) {
    return of(subDocument, SubDocumentValue.of(value));
  }

  public static SubDocumentUpdate of(final SubDocument subDocument, final Number[] value) {
    return of(subDocument, SubDocumentValue.of(value));
  }

  public static SubDocumentUpdate of(final SubDocument subDocument, final Boolean[] value) {
    return of(subDocument, SubDocumentValue.of(value));
  }

  public static SubDocumentUpdate of(final SubDocument subDocument, final SubDocumentValue value) {
    return new SubDocumentUpdate(subDocument, SET, value);
  }

  @SuppressWarnings("unused")
  public static class SubDocumentUpdateBuilder {
    private static final Set<UpdateOperator> VALUE_NON_REQUIRING_OPERATORS = Set.of(UNSET);
    private static final UpdateOperator DEFAULT_OPERATOR = SET;

    public SubDocumentUpdateBuilder subDocument(final String path) {
      subDocument = SubDocument.builder().path(path).build();
      return this;
    }

    public SubDocumentUpdateBuilder subDocument(final SubDocument subDocument) {
      this.subDocument = subDocument;
      return this;
    }

    public SubDocumentUpdate build() {
      setDefaultOperatorIfNull();
      setDefaultValueForValueNonRequiringOperators();

      Preconditions.checkNotNull(subDocument);
      Preconditions.checkNotNull(operator);
      Preconditions.checkNotNull(subDocumentValue);

      return new SubDocumentUpdate(subDocument, operator, subDocumentValue);
    }

    private void setDefaultOperatorIfNull() {
      if (operator == null) {
        operator = DEFAULT_OPERATOR;
      }
    }

    private void setDefaultValueForValueNonRequiringOperators() {
      if (VALUE_NON_REQUIRING_OPERATORS.contains(operator)) {
        if (subDocumentValue != null) {
          log.warn("Operator {} doesn't expect a value. Ignoring {}", operator, subDocumentValue);
        }

        subDocumentValue = new NullSubDocumentValue();
      }
    }
  }
}
