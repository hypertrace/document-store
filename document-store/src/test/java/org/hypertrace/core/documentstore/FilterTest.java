package org.hypertrace.core.documentstore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FilterTest {

  @Test
  public void testAnd() {
    Filter filter1 = new Filter();
    filter1.setFieldName("entity_type");
    filter1.setOp(Filter.Op.EQ);
    filter1.setValue("POD");

    Filter filter2 = new Filter();
    filter2.setFieldName("entity_id");
    filter2.setOp(Filter.Op.EQ);
    filter2.setValue("pod123");

    Filter filter = filter1.and(filter2);
    Assertions.assertEquals(2, filter.getChildFilters().length);
  }

  @Test
  public void testAnd2() {
    Filter filter1 = new Filter();
    filter1.setFieldName("entity_type");
    filter1.setOp(Filter.Op.EQ);
    filter1.setValue("SERVICE");

    Filter filter2 = new Filter();
    filter2.setFieldName("selector_key");
    filter2.setOp(Filter.Op.EQ);
    filter2.setValue("abc1");

    Filter filter3 = new Filter();
    filter3.setFieldName("selector_value");
    filter3.setOp(Filter.Op.EQ);
    filter3.setValue("abc2");

    Filter filter = filter1.and(filter2, filter3);
    Assertions.assertEquals(3, filter.getChildFilters().length);
  }

  @Test
  public void testSettingChildFiltersField() {
    Filter filter = new Filter();
    filter.setFieldName("entity_type");
    filter.setOp(Filter.Op.EQ);
    filter.setValue("SERVICE");

    Assertions.assertNull(filter.getChildFilters());

    Filter filter1 = Filter.eq("entity_type", "SERVICE");

    Assertions.assertNotNull(filter1.getChildFilters());
    Assertions.assertEquals(0, filter1.getChildFilters().length);

    Filter filter2 = new Filter(Filter.Op.GT, "duration", 60);

    Assertions.assertNotNull(filter2.getChildFilters());
    Assertions.assertEquals(0, filter2.getChildFilters().length);
  }

  @Test
  public void testFilterDate() {
    Filter filter = new Filter();

  }
}
