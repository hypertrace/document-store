package org.hypertrace.core.documentstore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class QueryTest {
  private Query query;

  @BeforeEach
  public void setup() {
    query = new Query();
  }

  @Test
  public void testAddAllOrderBys() {
    query.addAllOrderBys(List.of(new OrderBy("field1", true), new OrderBy("field2", false)));

    Assertions.assertEquals(2, query.getOrderBys().size());
    Assertions.assertEquals("field1", query.getOrderBys().get(0).getField());
    Assertions.assertTrue(query.getOrderBys().get(0).isAsc());

    Assertions.assertEquals("field2", query.getOrderBys().get(1).getField());
    Assertions.assertFalse(query.getOrderBys().get(1).isAsc());
  }

  @Test
  public void testAddOrderBy() {
    query.addOrderBy(new OrderBy("field1", true));

    Assertions.assertEquals(1, query.getOrderBys().size());
    Assertions.assertEquals("field1", query.getOrderBys().get(0).getField());
    Assertions.assertTrue(query.getOrderBys().get(0).isAsc());
  }

  @Test
  public void testEquals() {
    Query query1 = new Query();
    query1.setFilter(Filter.eq("entity_type", "SERVICE"));
    query1.addOrderBy(new OrderBy("field1", true));

    Query query2 = new Query();
    query2.setFilter(Filter.eq("entity_type", "SERVICE"));
    OrderBy orderBy = new OrderBy("field1", false);
    query2.addOrderBy(orderBy);
    Assertions.assertFalse(query1.equals(query2));

    orderBy.setIsAsc(true);
    Assertions.assertTrue(query1.equals(query2));
  }
}
