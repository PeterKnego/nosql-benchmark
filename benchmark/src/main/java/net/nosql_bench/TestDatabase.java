package net.nosql_bench;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestDatabase extends Database {

	private AtomicInteger idCounter = new AtomicInteger(0);

	private ThreadLocal<Map<Key, Transaction>> transaction = new ThreadLocal<>();

	private Map<Key, Entity> db;

	private synchronized String createId() {
		return String.valueOf(idCounter.addAndGet(1));
	}

	private void updateLocalTransaction(Entity entity) {
		if (!isTransaction()) {
			throw new RuntimeException("Error:Transaction not started!");
		}
		if (entity.key.id == null) {
			entity.key.id = createId();
		}
		transaction.get().put(entity.key, new Transaction("update", entity));
	}

	private void removeFromLocalTransaction(Key key) {
		if (!isTransaction()) {
			throw new RuntimeException("Error:Transaction not started!");
		}
		if (key.id == null) {
			key.id = createId();
		}
		transaction.get().put(key, new Transaction("delete", key));
	}

	private synchronized Entity getEntity(Key key) {
		return db.get(key);
	}

	private synchronized void transactionallyUpdateDB(Collection<Transaction> transactions) throws IllegalStateException {
		transactionallyUpdateDB(transactions.toArray(new Transaction[transactions.size()]));
	}

	private synchronized void transactionallyUpdateDB(Transaction... transactions) throws IllegalStateException {
		for (Transaction trans : transactions) {
			Entity changed = trans.entity;
			if (changed.key.id != null) {
				Entity original = db.get(changed.key);
				if (original == null && changed.version != 0) {   // new entity has version!=0 (most probably original was deleted)
					throw new IllegalStateException("New entity has version=" + changed.version);
				} else if (original != null && changed.version != original.version) {
					throw new IllegalStateException("Entity(" + changed.key + ") version has changed. changed=" + changed.version + " original=" + original.version);
				}
			} else {
				changed.key.id = createId(); // generate new unique ID
			}
		}
		for (Transaction trans : transactions) {
			if (trans.operation.equals("update")) {
				db.put(trans.entity.key, trans.entity);
			} else if (trans.operation.equals("update")) {
				db.remove(trans.entity.key);
			}
		}
	}

	private boolean isTransaction() {
		return transaction.get() != null;
	}

	@Override
	public void init(Properties props) {
		db = new HashMap<>(1000);
	}

	@Override
	public void register(String tableName, List<FieldDefinition> fields) {
		// no op
	}

	@Override
	public void cleanup(String tableName) {
		db = new HashMap<>(1000);
	}

	@Override
	public void finish() {
		db = null;
	}

	@Override
	public void startTransaction() {
		if (isTransaction()) {
			throw new IllegalStateException("Transaction already started");

		}
		transaction.set(new HashMap<Key, Transaction>(5));
	}

	@Override
	public void commitTransaction() {

		// transaction active?
		if (isTransaction()) {
			transactionallyUpdateDB(transaction.get().values());
		}

		transaction.remove();
	}

	@Override
	public void rollbackTransaction() {
		transaction.remove();
	}

	@Override
	public String insert(String tableName, Map<String, Object> fields) {

		Entity entity = new Entity(tableName, fields);

		// transaction active?
		if (isTransaction()) {
			transaction.get().put(entity.key, new Transaction("update", entity));
		} else {
			transactionallyUpdateDB(new Transaction("update", entity));
		}
		return null;
	}

	@Override
	public Map<String, Object> get(String stringKey) {
		Entity entity;
		Key key = Key.fromString(stringKey);
		if (isTransaction()) {
			entity = transaction.get().get(key).entity;
		} else {
			entity = getEntity(key);
		}
		return entity == null ? null : entity.fields;
	}

	@Override
	public void put(String tableName, String stringKey, Map<String, Object> fields) {
		Key key = Key.fromString(stringKey);
		Entity entity = new Entity(key, fields);
		if (isTransaction()) {
			updateLocalTransaction(entity);
		} else {
			transactionallyUpdateDB(new Transaction("update", entity));
		}
	}

	@Override
	public void delete(String tableName, String stringKey) {
		Key key = Key.fromString(stringKey);
		if (isTransaction()) {
			removeFromLocalTransaction(key);
		} else {
			transactionallyUpdateDB(new Transaction("delete", key));
		}
	}

	@Override
	public Map<String, Map<String, Object>> querySimple(String tableName, List<QueryPredicate> predicates, int skip, int limit) {
		Map<String, Map<String, Object>> results = new HashMap<String, Map<String, Object>>();
		for (Entity entity : db.values()) {
			boolean matched = true;
			for (QueryPredicate predicate : predicates) {
				matched = matched && matchedField(entity, predicate);
			}
			if (matched) {
				results.put(entity.key.toString(), entity.fields);
			}
		}
		return results;
	}

	private boolean matchedField(Entity entity, QueryPredicate predicate) {
		Object fieldVal = entity.fields == null ? null : entity.fields.get(predicate.fieldName);
		Object value = predicate.value;

		switch (predicate.operator) {
			case EQUALS:
				return value.equals(fieldVal);
			case GREATER:
				return greater(fieldVal, value);
			case GREATER_EQUALS:
				return greaterEquals(fieldVal, value);
			case LESSER:
				return lesser(fieldVal, value);
			case LESSER_EQUALS:
				return lesserEquals(fieldVal, value);
			case LIKE:
				if (fieldVal instanceof String && value instanceof String) {
					return ((String) fieldVal).contains((String) value);
				} else {
					throw new IllegalArgumentException("Query operator LIKE requires both field and value to be String.");
				}
			case CONTAINS:
				if (fieldVal instanceof Collection) {
					return ((Collection) fieldVal).contains(value);
				} else {
					throw new IllegalArgumentException("Query operator CONTAINS can only be used on fields of Collection types");
				}
		}
		return false;
	}

	private Number toNumber(Object obj) {
		return (obj instanceof Number) ? (Number) obj : null;
	}

	private boolean greaterEquals(Object field, Object value) {
		if (!field.getClass().equals(value.getClass())) {
			throw new IllegalArgumentException("Query value and field value do not have matching types.");
		}

		Number numField = toNumber(field);
		if (numField == null) {
			throw new IllegalArgumentException("Query operator GREATER can not be used on non-numeric fields.");
		}

		Number numValue = toNumber(field);
		if (numValue == null) {
			throw new IllegalArgumentException("Query operator GREATER can not be used with non-numeric values.");
		}
		return numField.doubleValue() > numValue.doubleValue();
	}

	private boolean greater(Object field, Object value) {
		if (!field.getClass().equals(value.getClass())) {
			throw new IllegalArgumentException("Query value and field value do not have matching types.");
		}

		Number numField = toNumber(field);
		if (numField == null) {
			throw new IllegalArgumentException("Query operator GREATER can not be used on non-numeric fields.");
		}

		Number numValue = toNumber(field);
		if (numValue == null) {
			throw new IllegalArgumentException("Query operator GREATER can not be used with non-numeric values.");
		}
		return numField.doubleValue() > numValue.doubleValue();
	}

	private boolean lesser(Object field, Object value) {
		if (!field.getClass().equals(value.getClass())) {
			throw new IllegalArgumentException("Query value and field value do not have matching types.");
		}

		Number numField = toNumber(field);
		if (numField == null) {
			throw new IllegalArgumentException("Query operator GREATER can not be used on non-numeric fields.");
		}

		Number numValue = toNumber(field);
		if (numValue == null) {
			throw new IllegalArgumentException("Query operator GREATER can not be used with non-numeric values.");
		}
		return numField.doubleValue() < numValue.doubleValue();
	}

	private boolean lesserEquals(Object field, Object value) {
		if (!field.getClass().equals(value.getClass())) {
			throw new IllegalArgumentException("Query value and field value do not have matching types.");
		}

		Number numField = toNumber(field);
		if (numField == null) {
			throw new IllegalArgumentException("Query operator GREATER can not be used on non-numeric fields.");
		}

		Number numValue = toNumber(field);
		if (numValue == null) {
			throw new IllegalArgumentException("Query operator GREATER can not be used with non-numeric values.");
		}
		return numField.doubleValue() <= numValue.doubleValue();
	}

	public static class Key {
		public String kind;
		public String id;

		public Key(String kind) {
			this.kind = kind;
		}

		public Key(String kind, String id) {
			this.kind = kind;
			this.id = id;
		}

		@Override
		public boolean equals(Object obj) {
			return (Key.class.equals(obj.getClass())) && ((Key) obj).id.equals(this.id) && ((Key) obj).kind.equals(this.kind);
		}

		public static Key createNew(String kind) {
			return new Key(kind);
		}

		public static Key fromString(String stringKey) {
			String[] keyParts = stringKey.split(":");
			return new Key(keyParts[0], keyParts[1]);
		}

		@Override
		public String toString() {
			return kind + ":" + id;
		}
	}

	public static class Entity {

		public Entity(Key key, Map<String, Object> fields) {
			this.key = key;
			this.fields = fields;
		}

		public Entity(String kind, Map<String, Object> fields) {
			this.key = Key.createNew(kind);
			this.fields = fields;
		}

		public Key key;

		// new entitieas have version=0
		private int version = 0;

		public Map<String, Object> fields = new HashMap<>();
	}

	public static class Transaction {
		public Entity entity;
		public String operation;

		public Transaction(String operation, Entity entity) {
			this.operation = operation;
			this.entity = entity;
		}

		public Transaction(String operation, Key key) {
			this.operation = operation;
			this.entity = new Entity(key, null);
		}
	}

}
