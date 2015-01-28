package net.nosql_bench;


import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.util.*;

public class OrientDb extends Database {

	//	private OPartitionedDatabasePool dbPool;
	private OSchema schema;

	private static ThreadLocal<ODatabaseDocumentTx> threadLocalDb = new ThreadLocal<>();

	@Override
	public void init(Properties props) {

		this.properties = props;
//		dbPool = new OPartitionedDatabasePoolFactory().get(
//				properties.getProperty("database"),
//				properties.getProperty("username"),
//				properties.getProperty("password"));

//		ODatabase db = dbPool.acquire();
		ODatabaseDocumentTx db = threadInit();
		schema = db.getMetadata().getSchema();
	}

	@Override
	public void cleanup(String tableName) {
		schema.dropClass(tableName);
	}

	private ODatabaseDocumentTx threadInit() {

		ODatabaseDocumentTx db = null;
		if (threadLocalDb.get() == null) {
			db = new ODatabaseDocumentTx(properties.getProperty("database")).open(properties.getProperty("username"), properties.getProperty("password"));
			threadLocalDb.set(db);
		}
		db = threadLocalDb.get();

//		ODatabaseDocumentTx db = dbPool.acquire();
		System.out.println("Thread: " + Thread.currentThread().getName() + " db:" + db.toString());
		return db;
	}

	@Override
	public void register(String tableName, List<FieldDefinition> fields) {
		OClass oldCls = schema.getClass(tableName);
		OClass cls = schema.getOrCreateClass(tableName);
		for (FieldDefinition field : fields) {
			if (!cls.existsProperty(field.fieldName)) {
				cls.createProperty(field.fieldName, translateFieldType(field));
				cls.createIndex(tableName + "." + field.fieldName, translateIndexType(field), field.fieldName);
			}
		}
	}

	@Override
	public void finish() {
		ODatabaseDocumentTx db = threadInit();
		db.close();
		threadLocalDb.remove();
	}

	@Override
	public void startTransaction() {
		ODatabaseDocumentTx db = threadInit();
		db.begin();
	}

	@Override
	public void commitTransaction() {
		ODatabaseDocumentTx db = threadInit();
		db.commit();
		threadLocalDb.remove();
	}

	@Override
	public void rollbackTransaction() {
		ODatabaseDocumentTx db = threadInit();
		db.rollback();
		threadLocalDb.remove();

	}

	@Override
	public String insert(String tableName, Map<String, Object> fields) {
		ODatabaseDocumentTx db = threadInit();
		ODocument doc = new ODocument(tableName);
		for (Map.Entry<String, Object> field : fields.entrySet()) {
			doc.field(field.getKey(), field.getValue());
		}
		ORecord record = db.save(doc);
		ORID orid = record.getIdentity();
		return fromORID(record);
	}

	@Override
	public Map<String, Object> get(String key) {
		ODatabaseDocumentTx db = threadInit();
		ODocument oldDoc = new ODocument();
		oldDoc.fromJSON(key);
		ODocument doc = db.load(oldDoc);
		return doc.toMap();
	}

	@Override
	public void put(String tableName, String key, Map<String, Object> fields) {
		ODatabaseDocumentTx db = threadInit();
		ODocument oldDoc = new ODocument();
		oldDoc.fromJSON(key);
		ODocument doc = db.load(oldDoc, null, true);
		for (Map.Entry<String, Object> field : fields.entrySet()) {
			doc.field(field.getKey(), field.getValue());
		}
		db.save(doc);
	}

	@Override
	public void delete(String tableName, String key) {
		ODatabaseDocumentTx db = threadInit();
		ODocument oldDoc = new ODocument();
		oldDoc.fromJSON(key);
		db.delete(oldDoc);
	}

	@Override
	public Map<String /*key*/, Map<String, Object> /*fields*/> querySimple(String tableName, List<QueryPredicate> predicates, int skip, int limit) {
		ODatabaseDocumentTx db = threadInit();

		StringBuilder queryString = new StringBuilder("select * from " + tableName + " where ");
		Iterator<QueryPredicate> iterator = predicates.iterator();
		while (iterator.hasNext()) {
			QueryPredicate predicate = iterator.next();
			queryString.append(predicate.fieldName);
			switch (predicate.operator) {
				case EQUALS:
					queryString.append(" == ").append(asQueryParameter(predicate.value));
					break;
				case NOT_EQUALS:
					queryString.append(" <> ").append(asQueryParameter(predicate.value));
					break;
				case CONTAINS:
					queryString.append(" contains ").append(predicate.value);
					break;
				case LIKE:
					queryString.append(" like ").append("'%").append(predicate.value).append("%'");
					break;
				case GREATER:
					queryString.append(" > ").append(asQueryParameter(predicate.value));
					break;
				case GREATER_EQUALS:
					queryString.append(" >= ").append(asQueryParameter(predicate.value));
					break;
				case LESSER:
					queryString.append(" < ").append(asQueryParameter(predicate.value));
					break;
				case LESSER_EQUALS:
					queryString.append(" <= ").append(asQueryParameter(predicate.value));
					break;
				default:
					throw new IllegalStateException("Unknown query operator: " + predicate.operator);
			}

			if (iterator.hasNext()) {
				queryString.append(" and");
			}
		}

		if (skip != 0) {
			queryString.append(" SKIP ").append(skip);
		}

		if (limit != 0) {
			queryString.append(" LIMIT ").append(limit);
		}

//		System.out.println("Query string: " + queryString.toString());

		List<ODocument> res = db.query(new OSQLSynchQuery<>(queryString.toString()));
		return queryResultToMap(res);
	}

	private String asQueryParameter(Object value) {
		if (value instanceof Integer) {
			return value.toString();
		} else if (value instanceof Long) {
			return value.toString();
		} else if (value instanceof Double) {
			return value.toString();
		} else if (value instanceof Float) {
			return value.toString();
		} else if (value instanceof String) {
			return "'" + value.toString() + "'";
		}
		throw new IllegalArgumentException("Unknown query param type: " + value.getClass());
	}

	private OType translateFieldType(FieldDefinition fieldDefinition) {

		switch (fieldDefinition.fieldType) {
			case STRING:
				return OType.STRING;
			case LONG:
				return OType.LONG;
			case INTEGER:
				return OType.INTEGER;
			case DOUBLE:
				return OType.DOUBLE;
			case SHORT:
				return OType.SHORT;
			case BYTE:
				return OType.BYTE;
			default:
				throw new IllegalArgumentException("Unknown field type: " + fieldDefinition.fieldType);
		}

	}

	private OClass.INDEX_TYPE translateIndexType(FieldDefinition fieldDefinition) {

		switch (fieldDefinition.indexType) {
			case SINGLE:
				return OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX;
			case RANGE:
				return OClass.INDEX_TYPE.NOTUNIQUE;
			case FULLTEXT:
				return OClass.INDEX_TYPE.FULLTEXT;
			default:
				throw new IllegalArgumentException("Unknown index type: " + fieldDefinition.indexType);
		}
	}

	private String fromORID(ORecord record) {
		return record.toJSON();
	}

	private Map<String /*key*/, Map<String, Object> /*fields*/> queryResultToMap(List<ODocument> results) {
		Map<String /*key*/, Map<String, Object> /*fields*/> out = new HashMap<>(results.size());
		for (ODocument result : results) {
			out.put(fromORID(result), result.toMap());
		}
		return out;
	}

}
