package org.hypertrace.core.documentstore.model.subdoc;

import static lombok.AccessLevel.PRIVATE;

import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor(access = PRIVATE)
public class SubDocumentUpdate {
  SubDocument subDocument;
  SubDocumentValue subDocumentValue;

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
    return new SubDocumentUpdate(SubDocument.builder().path(subDocumentPath).build(), value);
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
    return new SubDocumentUpdate(subDocument, value);
  }
}
