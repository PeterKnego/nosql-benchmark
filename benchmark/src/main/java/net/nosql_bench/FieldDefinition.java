package net.nosql_bench;

public class FieldDefinition {

	public enum FIELD_TYPE {STRING, LONG, DOUBLE}
	public enum INDEX_TYPE {SINGLE, RANGE, FULLTEXT}

	public String fieldName;
	public FIELD_TYPE fieldType;
	public INDEX_TYPE indexType;

	public FieldDefinition(String fieldName, FIELD_TYPE fieldType, INDEX_TYPE indexType) {
		this.fieldName = fieldName;
		this.fieldType = fieldType;
		this.indexType = indexType;
	}
}
