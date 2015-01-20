package net.nosql_bench;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class DbTest {

	protected Properties properties;

	public abstract void init(Properties props);

	public abstract void register(String tableName, List<FieldDefinition> fields);

	public abstract void cleanup(String tableName);

	public abstract void finish();

	public abstract void startTransaction();

	public abstract void commitTransaction();

	public abstract String insert(String tableName, Map<String, Object> fields);

	public abstract Map<String, Object> get(String key);

	public abstract void put(String tableName, String key, Map<String, Object> fields);

	public abstract void delete(String tableName, String key);

	public abstract int querySimple(String tableName, List<QueryPredicate> predicates, int skip, int limit);

}
