package net.nosql_bench;


import com.aerospike.client.*;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

import java.util.*;

public class Aerospike extends Database {

	// true if "transaction" is active
	private static ThreadLocal<Map<Key, AsRecord>> threadLocalTransaction = new ThreadLocal<Map<Key, AsRecord>>();

	private AerospikeClient db;
	private String namespace;

	@Override
	public void init(Properties props) {
		properties = props;
		String host = properties.getProperty("host");
		int port = PropsUtil.expandInt(properties.getProperty("port", "3000"));
		db = new AerospikeClient(host, port);


		namespace = properties.getProperty("namespace");
	}

	@Override
	public void register(String tableName, List<FieldDefinition> fields) {
		for (FieldDefinition field : fields) {
			String indexName = tableName + "_" + field.fieldName + "_" + field.indexType.name();
			IndexTask task = db.createIndex(null, namespace, tableName, indexName, field.fieldName, translateIndexType(field));
			task.waitTillComplete();
		}
	}

	@Override
	public void cleanup(String tableName) {
		// there is no "drop class" method
	}

	@Override
	public void finish() {
//		db.close();
	}

	@Override
	public void startTransaction() {
		threadLocalTransaction.set(new HashMap<Key, AsRecord>());
	}

	@Override
	public void commitTransaction() {
		threadLocalTransaction.set(null);
	}

	@Override
	public void rollbackTransaction() {
		threadLocalTransaction.set(null);
	}

	@Override
	public String insert(String tableName, Map<String, Object> fields) {

		// generate key
		Value.StringValue generatedId = new Value.StringValue(UUID.randomUUID().toString());
		Key key = new Key(namespace, tableName, generatedId);

		// save data
		put(key, fields);
		return fromKey(key, 0);
	}

	@Override
	public void put(String tableName, String keyUser, Map<String, Object> fields) {
		Key key = toKey(keyUser);
		put(key, fields);
	}

	private void put(Key key, Map<String, Object> fields) {
		WritePolicy writePolicy = new WritePolicy();

		if (threadLocalTransaction.get() != null) {
			writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL; // enforces simple transaction
			AsRecord existingRecord = threadLocalTransaction.get().get(key);
			writePolicy.generation = existingRecord == null ? 0 : existingRecord.generation;
		} else {
			writePolicy.generationPolicy = GenerationPolicy.NONE; // overwrite
		}

		Bin[] bins = toBins(fields);
		db.put(writePolicy, key, bins);
	}

	@Override
	public Map<String, Object> get(String userKey) {

		boolean isTransaction = threadLocalTransaction.get() != null;

		Key dbKey = toKey(userKey);
		Record record = db.get(null, dbKey);

		if (record != null && isTransaction) {
			threadLocalTransaction.get().put(dbKey, AsRecord.from(record));
		}

		return record == null ? null : record.bins;
	}

	@Override
	public void delete(String tableName, String keyUser) {
		Key key = toKey(keyUser);
		WritePolicy writePolicy = new WritePolicy();
		if (threadLocalTransaction.get() != null) {
			writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL; // enforces simple transaction
		} else {
			writePolicy.generationPolicy = GenerationPolicy.NONE; // overwrite
		}
		db.delete(writePolicy, key);
	}

	@Override
	public Map<String, Map<String, Object>> querySimple(String tableName, List<QueryPredicate> predicates, int skip, int limit) {
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(tableName);

		boolean isTransaction = threadLocalTransaction.get() != null;

		Set<Filter> filters = new HashSet<>();
		for (QueryPredicate predicate : predicates) {
			Value value = Value.get(predicate.value);
			switch (predicate.operator) {
				case EQUALS:
					if (value.getType() == ParticleType.INTEGER) {
						filters.add(Filter.equal(predicate.fieldName, value.toLong()));
					} else if (value.getType() == ParticleType.STRING) {
						filters.add(Filter.equal(predicate.fieldName, value.toString()));
					} else {
						throw new IllegalStateException("Unsupported query value type: " + value.getType() + ". Must be ParticleType.INTEGER or ParticleType.STRING.");
					}
					break;
				case NOT_EQUALS:
					if (value.getType() == ParticleType.INTEGER) {
						filters.add(Filter.range(predicate.fieldName, Long.MIN_VALUE, value.toLong()));
						filters.add(Filter.range(predicate.fieldName, value.toLong(), Long.MAX_VALUE));
					} else {
						throw new IllegalStateException("Unsupported query value type: " + value.getType() + ". Must be ParticleType.INTEGER.");
					}

				case CONTAINS:
					throw new IllegalStateException("Unsupported query operator: " + predicate.operator);
				case LIKE:
					throw new IllegalStateException("Unsupported query operator: " + predicate.operator);
				case GREATER:
					if (value.getType() == ParticleType.INTEGER) {
						filters.add(Filter.range(predicate.fieldName, value.toLong(), Long.MAX_VALUE));
					} else {
						throw new IllegalStateException("Unsupported query value type: " + value.getType() + ". Must be ParticleType.INTEGER.");
					}
					break;
				case GREATER_EQUALS:
					if (value.getType() == ParticleType.INTEGER) {
						filters.add(Filter.equal(predicate.fieldName, value.toLong()));
						filters.add(Filter.range(predicate.fieldName, value.toLong(), Long.MAX_VALUE));
					} else {
						throw new IllegalStateException("Unsupported query value type: " + value.getType() + ". Must be ParticleType.INTEGER.");
					}
					break;
				case LESSER:
					if (value.getType() == ParticleType.INTEGER) {
						filters.add(Filter.range(predicate.fieldName, Long.MIN_VALUE, value.toLong()));
					} else {
						throw new IllegalStateException("Unsupported query value type: " + value.getType() + ". Must be ParticleType.INTEGER.");
					}
					break;
				case LESSER_EQUALS:
					if (value.getType() == ParticleType.INTEGER) {
						filters.add(Filter.equal(predicate.fieldName, value.toLong()));
						filters.add(Filter.range(predicate.fieldName, Long.MIN_VALUE, value.toLong()));
					} else {
						throw new IllegalStateException("Unsupported query value type: " + value.getType() + ". Must be ParticleType.INTEGER.");
					}
					break;
				default:
					throw new IllegalStateException("Unknown query operator: " + predicate.operator);
			}

		}
		stmt.setFilters(filters.toArray(new Filter[filters.size()]));
		RecordSet results = db.query(db.queryPolicyDefault, stmt);

		Map<String, Map<String, Object>> out = new HashMap<>();

		while (results.next()) {
			Key key = results.getKey();
			Record record = results.getRecord();
			out.put(fromKey(key, record.generation), record.bins);

			if (record != null && isTransaction) {
				threadLocalTransaction.get().put(key, AsRecord.from(record));
			}
		}

		return out;
	}

	private IndexType translateIndexType(FieldDefinition fieldDefinition) {

		switch (fieldDefinition.fieldType) {
			case INTEGER:
				return IndexType.NUMERIC;
			case LONG:
				return IndexType.NUMERIC;
			case DOUBLE:
				return IndexType.NUMERIC;
			case SHORT:
				return IndexType.NUMERIC;
			case STRING:
				return IndexType.STRING;
			default:
				throw new IllegalArgumentException("Unknown index type: " + fieldDefinition.indexType);
		}
	}

	public static Bin[] toBins(Map<String, Object> fields) {
		Bin[] bins = new Bin[fields.size()];
		int i = 0;
		for (String fieldName : fields.keySet()) {
			Object fieldVal = fields.get(fieldName);
			bins[i] = new Bin(fieldName, fieldVal);
			i++;
		}
		return bins;
	}


	private Key toKey(String userKey) {
		String[] parts = userKey.split("#");
		String setName = parts[0];
		Value userKeyValue = new Value.StringValue(parts[1]);
		return new Key(namespace, setName, userKeyValue);
	}

	private int getGeneration(String userKey) {
		String[] parts = userKey.split("#");
		return Integer.valueOf(parts[2]);
	}

	private String fromKey(Key key, int generation) {
		return key.setName + "#" + key.userKey.toString() + "#" + generation;
	}

	public static class AsRecord {
		int generation;
		Map<String, Object> fields;

		public AsRecord(int generation, Map<String, Object> fields) {
			this.generation = generation;
			this.fields = fields;
		}

		public static AsRecord from(Record record) {
			return new AsRecord(record.generation, record.bins);
		}
	}
}
