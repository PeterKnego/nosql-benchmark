package net.nosql_bench;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestDatabase extends Database {

	private AtomicInteger idCounter = new AtomicInteger(0);

	private ThreadLocal<Map<Key, Transaction>> threadLocalTransaction = new ThreadLocal<>();

	private Map<Key, Entity> db;

	/**
	 * For test purposes only!
	 */
	void setTransaction(Map<Key, Transaction> transactionContext) {
		threadLocalTransaction.set(transactionContext);
	}

	/**
	 * For test purposes only!
	 */
	public Map<Key, Transaction> getTransaction() {
		return threadLocalTransaction.get();
	}

	private synchronized String createId() {
		return String.valueOf(idCounter.addAndGet(1));
	}

	private Key updateLocalTransaction(Entity entity) {

		entity = Entity.copy(entity);

		if (!isTransaction()) {
			throw new RuntimeException("Error:Transaction not started!");
		}
		if (entity.key.id == null) {
			entity.key = new Key(entity.key.kind, createId());
		}

		Transaction existing = threadLocalTransaction.get().get(entity.key);
		if (existing != null) {
			entity.version = existing.entity.version;
		}

		threadLocalTransaction.get().put(entity.key, new Transaction("update", entity));
		return entity.key;
	}

	private void removeFromLocalTransaction(Key key) {
		if (!isTransaction()) {
			throw new RuntimeException("Error:Transaction not started!");
		}
		if (key.id == null) {
			key = new Key(key.kind, createId());
		}
		threadLocalTransaction.get().put(key, new Transaction("delete", key));
	}

	private synchronized Entity getEntity(Key key) {
		return db.get(key);
	}

	private synchronized void transactionallyUpdateDB(Collection<Transaction> transactions, boolean enforceVersioning) throws IllegalStateException {
		transactionallyUpdateDB(enforceVersioning, transactions.toArray(new Transaction[transactions.size()]));
	}

	private synchronized void transactionallyUpdateDB(boolean enforceVersioning, Transaction... transactions) throws IllegalStateException {
		for (Transaction trans : transactions) {
			Entity changed = trans.entity;
			if (changed.key.id != null) {
				Entity original = db.get(changed.key);
				if (original != null && !enforceVersioning) {
					changed.version = original.version;
				}
				if (original == null && changed.version != 0) {   // new entity has version!=0 (most probably original was deleted)
					throw new IllegalStateException("New entity has version=" + changed.version);
				} else if (original != null && changed.version != original.version) {
					throw new IllegalStateException("Entity(" + changed.key + ") version has changed. changed=" + changed.version + " original=" + original.version);
				}
			} else {
				changed.key = new Key(changed.key.kind, createId()); // generate new unique ID
			}
		}
		for (Transaction trans : transactions) {
			if (trans.operation.equals("update")) {
				trans.entity.version++;
				db.put(trans.entity.key, trans.entity);
			} else if (trans.operation.equals("update")) {
				db.remove(trans.entity.key);
			}
		}
	}

	private synchronized Key transactionallyUpdateDB(Transaction trans, boolean enforceVersioning) throws IllegalStateException {
		Entity changed = trans.entity;
		if (changed.key.id != null) {
			Entity original = db.get(changed.key);
			if (original != null && !enforceVersioning) {
				changed.version = original.version;
			}
			if (original == null && changed.version != 0) {   // new entity has version!=0 (most probably original was deleted)
				throw new IllegalStateException("New entity has version=" + changed.version);
			} else if (original != null && changed.version != original.version) {
				throw new IllegalStateException("Entity(" + changed.key + ") version has changed. changed=" + changed.version + " original=" + original.version);
			}
		} else {
			changed.key = new Key(changed.key.kind, createId()); // generate new unique ID
		}

		if (trans.operation.equals("update")) {
			trans.entity.version++;
			db.put(trans.entity.key, trans.entity);
		} else if (trans.operation.equals("update")) {
			db.remove(trans.entity.key);
		}
		return trans.entity.key;
	}

	private boolean isTransaction() {
		return threadLocalTransaction.get() != null;
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

	}

	@Override
	public void startTransaction() {
		if (isTransaction()) {
			throw new IllegalStateException("Transaction already started");
		}
		threadLocalTransaction.set(new HashMap<Key, Transaction>(5));
	}

	@Override
	public void commitTransaction() {

		// transaction active?
		if (isTransaction()) {
			transactionallyUpdateDB(threadLocalTransaction.get().values(), true);
		}

		threadLocalTransaction.remove();
	}

	@Override
	public void rollbackTransaction() {
		threadLocalTransaction.remove();
	}

	@Override
	public String insert(String tableName, Map<String, Object> fields) {

		Entity entity = new Entity(tableName, fields);

		// transaction active?
		if (isTransaction()) {
			return updateLocalTransaction(entity).toString();
		} else {
			return transactionallyUpdateDB(new Transaction("update", entity), false).toString();
		}
	}

	@Override
	public Map<String, Object> get(String stringKey) {
		Entity entity = null;
		Key key = Key.fromString(stringKey);
		if (isTransaction()) {
			Transaction transaction = threadLocalTransaction.get().get(key);
			entity = transaction != null ? transaction.entity : null;
		}

		if (entity == null) {
			entity = getEntity(key);
		}

		// we need to remember all entities within the transaction
		if (isTransaction()) {
			updateLocalTransaction(entity);
		}
		return entity == null ? null : new HashMap<>(entity.fields);
	}

	@Override
	public void put(String tableName, String stringKey, Map<String, Object> fields) {
		Key key = Key.fromString(stringKey);
		Entity entity = new Entity(key, fields);
		if (isTransaction()) {
			updateLocalTransaction(entity);
		} else {
			transactionallyUpdateDB(new Transaction("update", entity), false);
		}
	}

	@Override
	public void delete(String tableName, String stringKey) {
		Key key = Key.fromString(stringKey);
		if (isTransaction()) {
			removeFromLocalTransaction(key);
		} else {
			transactionallyUpdateDB(new Transaction("delete", key), true);
		}
	}

	@Override
	public Map<String, Map<String, Object>> querySimple(String tableName, List<QueryPredicate> predicates, int skip, int limit) {
		Map<String, Map<String, Object>> results = new HashMap<String, Map<String, Object>>();

		synchronized (this) {
			for (Entity entity : db.values()) {
				boolean matched = true;
				for (QueryPredicate predicate : predicates) {
					matched = matched && matchedField(entity, predicate);
				}
				if (matched) {
					if (skip > 0) {
						skip--;
					} else if (limit > 0) {
						limit--;
						// we need to remember all entities within the transaction
						if (isTransaction()) {
							updateLocalTransaction(entity);
						}
						results.put(entity.key.toString(), new HashMap<>(entity.fields));
					} else {
						break;
					}
				}
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
			case NOT_EQUALS:
				return !value.equals(fieldVal);
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

		Number numValue = toNumber(value);
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

		Number numValue = toNumber(value);
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

		Number numValue = toNumber(value);
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

		Number numValue = toNumber(value);
		if (numValue == null) {
			throw new IllegalArgumentException("Query operator GREATER can not be used with non-numeric values.");
		}
		return numField.doubleValue() <= numValue.doubleValue();
	}

	public static class Key {
		private final String kind;
		private final String id;

		public Key(String kind) {
			this.kind = kind;
			id = null;
		}

		public Key(String kind, String id) {
			this.kind = kind;
			this.id = id;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			Key key = (Key) o;

			return id.equals(key.id) && kind.equals(key.kind);
		}

		@Override
		public int hashCode() {
			return 31 * kind.hashCode() + id.hashCode();
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

		public static Key copy(Key key) {
			return new Key(key.kind, key.id);
		}
	}

	public static class Entity {

		public Key key;
		public Map<String, Object> fields = new HashMap<>();
		private int version = 0;  // new entities have version=0

		public Entity(Key key, Map<String, Object> fields) {
			this.key = key;
			this.fields = new HashMap<>(fields);
		}

		public Entity(String kind, Map<String, Object> fields) {
			this.key = Key.createNew(kind);
			this.fields = new HashMap<>(fields);
		}

		public Entity(Key copy, int version, HashMap<String, Object> fields) {
			key = copy;
			this.version = version;
			this.fields = new HashMap<>(fields);;
		}

		public static Entity copy(Entity entity) {
			return new Entity(Key.copy(entity.key), entity.version, new HashMap<>(entity.fields));
		}
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
