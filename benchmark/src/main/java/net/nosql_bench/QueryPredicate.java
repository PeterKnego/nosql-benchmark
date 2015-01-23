package net.nosql_bench;

public class QueryPredicate {
	public enum OPERATOR {EQUALS, NOT_EQUALS, GREATER_EQUALS, LESSER_EQUALS, GREATER, LESSER, LIKE, CONTAINS}

	public String fieldName;
	public OPERATOR operator;
	public Object value;

	public QueryPredicate(String fieldName, OPERATOR operator, Object value) {
		this.fieldName = fieldName;
		this.operator = operator;
		this.value = value;
	}
}
