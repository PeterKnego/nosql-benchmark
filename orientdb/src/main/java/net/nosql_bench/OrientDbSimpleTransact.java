package net.nosql_bench;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class OrientDbSimpleTransact {

	private static final AtomicInteger verifyCounter = new AtomicInteger(0);
	private static final AtomicInteger collisions = new AtomicInteger(0);

	public static void main(String[] args) throws InterruptedException {

		ODatabaseDocument db = new ODatabaseDocumentTx("remote:localhost/test")
				.open("admin", "admin");
		OSchema schema = db.getMetadata().getSchema();

		System.out.println("MVCC: " + db.isMVCC());

		String tableName = "Counter";
		int threads = 5;
		int repeat = 20;

		// register class
		OClass cls = schema.getOrCreateClass(tableName);
		String fieldName = "number";
		if (!cls.existsProperty(fieldName)) {
			cls.createProperty(fieldName, OType.INTEGER);
		}

		// create counter entity
		ODocument doc = new ODocument(tableName);
		doc.field(fieldName, 0);
		ORID orid = db.save(doc).getIdentity();

		// starting threads
		ExecutorService executor = Executors.newFixedThreadPool(100);
		Collection<Callable<Void>> tasks = new ArrayList<>();
		for (int n = 1; n <= threads; n++) {

			int delta = 1;
			tasks.add(new CounterIncrement(orid, repeat));
			System.out.println("Added task:" + n + " delta:" + delta);
		}
		executor.invokeAll(tasks);
		executor.shutdown();

		// load the counter again
		ODocument res = db.load(orid, null, true);

		System.out.println("Counter updates: " + res.toMap().get(fieldName));
		System.out.println("Verify counter: " + verifyCounter);
		System.out.println("Collisions: " + collisions);
		if (res.toMap().get(fieldName) != verifyCounter) {
			System.out.println("Error: number of updates (" +
					res.toMap().get(fieldName) + ") is not equal to verify counter (" + verifyCounter + ").");
		}
		db.close();

	}

	public static class CounterIncrement implements Callable<Void> {

		public CounterIncrement(ORID key, int repeat) {
			this.key = key;
			this.repeat = repeat;
		}

		private ORID key;
		private int repeat;

		/**
		 * Transactionally increments the counter
		 */
		@Override
		public Void call() throws Exception {

			ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/test")
					.open("admin", "admin");
			while (repeat != 0) {

				try {
					db.begin(OTransaction.TXTYPE.OPTIMISTIC);
					ODocument entity = db.load(key, null, true);

					if (entity != null) {

						// increment field 'number' by one
						int val = entity.field("number");
						val++;
						entity.field("number", val);

						db.save(entity);
						db.commit();
						verifyCounter.addAndGet(1);

						// repeat variable is only decreased when transaction is committed successfully
						repeat--;
						System.out.println("Updated entity:" + key + " number:" + entity.field("number"));
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
			db.close();

			return null;
		}
	}
}


