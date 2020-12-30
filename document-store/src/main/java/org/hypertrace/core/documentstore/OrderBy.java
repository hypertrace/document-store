package org.hypertrace.core.documentstore;

public class OrderBy {

    private String field;
    private boolean isAsc;

    public OrderBy(String field, boolean isAsc) {
        this.field = field;
        this.isAsc = isAsc;
    }


    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public boolean isAsc() {
        return isAsc;
    }

    public void setIsAsc(boolean isAsc) {
        this.isAsc = isAsc;
    }
}
