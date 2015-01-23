package net.nosql_bench;

import java.util.*;

public class ACID {

	public static void main(String[] args) {
		TestDatabase test = new TestDatabase();
		new ACID().overlappingTransactions(test);
	}

	public void overlappingTransactions(TestDatabase test) {

		String tableName = "Test";

		// initialise the db
		test.init(null);

		// register entities
		List<FieldDefinition> fieldDef = new ArrayList<>();
		fieldDef.add(new FieldDefinition("number", FieldDefinition.FIELD_TYPE.INTEGER, FieldDefinition.INDEX_TYPE.RANGE));
		test.register(tableName, fieldDef);

		// create test entities
		Map<String, Object> fields = new HashMap<>(2);
		fields.put("number", 10);
		String key1 = test.insert(tableName, fields);

		// test overlapping transactions
		test.startTransaction();
		Map<TestDatabase.Key, TestDatabase.Transaction> transaction1 = test.getTransaction();
		Map<String, Object> fields1 = test.get(key1);

		test.setTransaction(null);
		test.startTransaction();
		Map<TestDatabase.Key, TestDatabase.Transaction> transaction2 = test.getTransaction();
		Map<String, Object> fields2 = test.get(key1);

		test.setTransaction(transaction1);
		fields1.put("number", 1);
		test.put(tableName, key1, fields1);
		test.commitTransaction();

		test.setTransaction(transaction2);
		fields2.put("number", 2);
		test.put(tableName, key1, fields2);
		test.commitTransaction();

		System.out.println();

	}
}
