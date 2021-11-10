package org.hypertrace.core.documentstore.expression;

import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;

public class Utils {

  private static final Validator validator =
      Validation.buildDefaultValidatorFactory().getValidator();

  public static <T> T validateAndReturn(T expression) {
    Set<ConstraintViolation<T>> exceptions = validator.validate(expression);
    if (!exceptions.isEmpty()) {
      throw new IllegalArgumentException(exceptions.iterator().next().getMessage());
    }

    return expression;
  }
}
