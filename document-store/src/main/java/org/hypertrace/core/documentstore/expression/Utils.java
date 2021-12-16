package org.hypertrace.core.documentstore.expression;

import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;

public class Utils {

  private static final Validator validator =
      Validation.byDefaultProvider()
          .configure()
          .messageInterpolator(new ParameterMessageInterpolator())
          .buildValidatorFactory()
          .getValidator();

  public static <T> T validateAndReturn(final T expression) {
    Set<ConstraintViolation<T>> exceptions = validator.validate(expression);
    if (!exceptions.isEmpty()) {
      throw new IllegalArgumentException(exceptions.iterator().next().getMessage());
    }

    return expression;
  }
}
