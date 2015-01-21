package net.nosql_bench.workloads;

import net.nosql_bench.*;

import java.util.*;

public class ACID {

	public static void main(String[] args) {
		TestDatabase test = new TestDatabase();
		new ACID().execute(test);
	}

	public void execute(Database test) {

		String tableName = "Test";

		// initialise the db
		test.init(null);

		// register entities
		List<FieldDefinition> fieldDef = new ArrayList<>();
		fieldDef.add(new FieldDefinition("number", FieldDefinition.FIELD_TYPE.INTEGER, FieldDefinition.INDEX_TYPE.RANGE));
		test.register(tableName, fieldDef);

		test.startTransaction();

		Map<String, Object> fields = new HashMap<>(2);
		fields.put("number", 10);
		test.insert(tableName, fields);

		fields = new HashMap<>(2);
		fields.put("number", 1);
		test.insert(tableName, fields);

		test.commitTransaction();

		List<QueryPredicate> predicates = new ArrayList<QueryPredicate>(1);
		predicates.add(new QueryPredicate("number", QueryPredicate.OPERATOR.LESSER_EQUALS, 10));
		Map<String, Map<String, Object>> res = test.querySimple(tableName, predicates, 0, 0);

		System.out.println();


	}
}
