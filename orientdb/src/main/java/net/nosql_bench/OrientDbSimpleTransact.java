package net.nosql_bench;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class OrientDbSimpleTransact {

	private static final AtomicInteger testInt = new AtomicInteger(0);
	private static final AtomicInteger collisions = new AtomicInteger(0);

	public static void main(String[] args) {

		ODatabaseDocument db = new ODatabaseDocumentTx("remote:localhost/test")
				.open("admin", "admin");
		OSchema schema = db.getMetadata().getSchema();

		System.out.println("MVCC: " + db.isMVCC());

		String tableName = "Test";
		int threads = 20;
		int repeat = 10;

		OClass cls = schema.getOrCreateClass(tableName);
		String fieldName = "number";
		if (!cls.existsProperty(fieldName)) {
			cls.createProperty(fieldName, OType.INTEGER);
		}
//		cls.createIndex(tableName + ".number", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, fieldName);

		// create initial entity
		ODocument doc = new ODocument(tableName);
		doc.field(fieldName, 0);
		ORID orid = db.save(doc).getIdentity();

		// starting threads
		ScenarioExecutor<Void> executor = new ScenarioExecutor<Void>(100);
		for (int n = 1; n <= threads; n++) {

			int delta = 1;
			executor.addTask(new TransactTask(orid, delta, repeat));
			System.out.println("Added task:" + n + " delta:" + delta);
		}
		executor.start();
		executor.getResults();

		// get initial entity
		ODocument res = db.load(orid, null, true);

		System.out.println("Result: (" + orid + ") updates: " + res.toMap().get(fieldName));
		System.out.println("Test counter: " + testInt);
		System.out.println("Collisions: " + collisions);
		if (res.toMap().get(fieldName) != testInt) {
			System.out.println("Error: number of updates (" +
					res.toMap().get(fieldName) + ") is not equal to test counter (" + collisions + ").");
		}

	}

	public static class TransactTask implements Callable<List<Void>> {

		public TransactTask(ORID key, int delta, int repeat) {
			this.key = key;
			this.delta = delta;
			this.repeat = repeat;
		}

		private ORID key;
		private int delta;
		private int repeat;

		@Override
		public List<Void> call() throws Exception {

			while (repeat != 0) {
				ODatabaseDocument db = new ODatabaseDocumentTx("remote:localhost/test")
						.open("admin", "admin");
				try {
					db.begin();
					ODocument entity = db.load(key, null, true);

					if (entity != null) {

						// change field 'number' by delta
						int val = entity.field("number");
						val += delta;
						entity.field("number", val);

						db.save(entity);
						db.commit();
						testInt.addAndGet(delta);
						repeat--;
						System.out.println("Updated " + delta + " entity:" + key + " number:" + entity.field("number"));
					} else {
						System.out.println("transact " + Thread.currentThread().getName() + " Not found! key=" + key);
						db.rollback();
					}
				} catch (RuntimeException re) {
					collisions.addAndGet(1);
					System.out.println("Collision: " + re.getMessage());
					db.rollback();
				}
			}

			return null;
		}
	}

}

