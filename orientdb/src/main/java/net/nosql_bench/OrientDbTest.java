package net.nosql_bench;


import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OrientDbTest extends DbTest {

	private OSchema schema;


	@Override
	public void init(Properties props) {
		this.properties = props;
		ODatabase db = new ODatabaseDocumentTx(properties.getProperty("orientdb.database"))
				.open(properties.getProperty("orientdb.username"), properties.getProperty("orientdb.password"));
		schema = db.getMetadata().getSchema();
	}

	private ODatabaseDocument threadInit() {
		if (!ODatabaseRecordThreadLocal.INSTANCE.isDefined()) {
			ODatabaseDocumentInternal db = new ODatabaseDocumentTx(properties.getProperty("orientdb.database"))
					.open(properties.getProperty("orientdb.username"), properties.getProperty("orientdb.password"));
			ODatabaseRecordThreadLocal.INSTANCE.set(db);
		}
		return ODatabaseRecordThreadLocal.INSTANCE.get();
	}

	@Override
	public void register(String tableName, List<FieldDefinition> fields) {
		OClass oldCls = schema.getClass(tableName);
		if (oldCls != null) {
			schema.dropClass(tableName);
		}
		OClass cls = schema.createClass(tableName);
		for (FieldDefinition field : fields) {
			cls.createProperty(field.fieldName, translateFieldType(field));
			cls.createIndex(tableName + "." + field.fieldName, translateIndexType(field), field.fieldName);
		}
	}

	@Override
	public void finish() {
//		db.close();
	}

	@Override
	public void startTransaction() {

	}

	@Override
	public void commitTransaction() {

	}

	@Override
	public String insert(String tableName, Map<String, Object> fields) {
		ODatabaseDocument db = threadInit();
		ODocument doc = new ODocument(tableName);
		for (Map.Entry<String, Object> field : fields.entrySet()) {
			doc.field(field.getKey(), field.getValue());
		}
		ORID orid = db.save(doc).getIdentity();
		return fromORID(orid);
	}

	@Override
	public Map<String, Object> get(String key) {
		ODatabaseDocument db = threadInit();
		ODocument doc = db.load(toORID(key));
		return doc.toMap();
	}

	@Override
	public void put(String tableName, String key, Map<String, Object> fields) {
		ODatabaseDocument db = threadInit();
		ODocument doc = db.load(toORID(key));
		for (Map.Entry<String, Object> field : fields.entrySet()) {
			doc.field(field.getKey(), field.getValue());
		}
		db.save(doc);
	}

	@Override
	public void delete(String tableName, String key) {
		ODatabaseDocument db = threadInit();
		ORID orid = toORID(key);
		db.delete(orid);
	}

	@Override
	public int querySimple(String tableName, List<QueryPredicate> predicates) {
		ODatabaseDocument db = threadInit();

		StringBuilder queryString = new StringBuilder("select * from " + tableName + " where ");
		for (QueryPredicate predicate : predicates) {
			queryString.append(predicate.fieldName);
			switch (predicate.operator) {
				case EQUALS:
					queryString.append(" == ").append(asQueryParameter(predicate.value));
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

		}

		List<ODocument> res = db.query(new OSQLSynchQuery<>(queryString.toString()));
		return res.size();
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
			case DOUBLE:
				return OType.DOUBLE;
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

	private ORID toORID(String oridString) {
		int separatorIndex = oridString.indexOf('#');
		int clusterId = Integer.valueOf(oridString.substring(0, separatorIndex));
		int clusterPosition = Integer.valueOf(oridString.substring(separatorIndex + 1));
		return new ORecordId(clusterId, clusterPosition);
	}

	private String fromORID(ORID orid) {
		return orid.getClusterId() + "#" + orid.getClusterPosition();
	}

}
