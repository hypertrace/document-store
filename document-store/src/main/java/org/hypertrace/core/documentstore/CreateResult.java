package org.hypertrace.core.documentstore;

/*
 * Represent the result object for CREATE operation of document store APIs.
 * */
public class CreateResult {
  private boolean succeed;

  public CreateResult(boolean succeed) {
    this.succeed = succeed;
  }

  public boolean isSucceed() {
    return succeed;
  }
}
