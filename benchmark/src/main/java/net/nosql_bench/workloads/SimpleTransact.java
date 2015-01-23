package net.nosql_bench.workloads;

import net.nosql_bench.*;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleTransact implements Workload {

	private String tableName;
	private int initialValue = 1000;

	@Override
	public void execute(Database test, Properties dbProperties, Properties workloadProperties) {

		tableName = workloadProperties.getProperty("tablename");
		boolean cleanup = Boolean.valueOf(workloadProperties.getProperty("cleanup", "false"));
		int threads = PropsUtil.expandInt(workloadProperties.getProperty("threads", "1"));

		int repeat = PropsUtil.expandInt(workloadProperties.getProperty("transact.repeat", "100"));

		setup(test, dbProperties);
		String key = createInitialEntity(test);

		System.out.println("Starting transact..");
		long queryDuration = transact(test, threads, key, repeat);
		System.out.println("Transact benchmark: repeats=" + repeat + " duration=" + queryDuration + " rate=" + ((1000 * repeat) / queryDuration));

		Map<String, Object> res = test.get(key);

		System.out.println("Result: number=" + res.get("number"));

		if (cleanup) {
			System.out.println("Cleanup..");
			test.cleanup(tableName);
		}
	}

	public void setup(final Database test, Properties props) {

		// initialise the db
		test.init(props);

		// register entities
		List<FieldDefinition> fieldDef = new ArrayList<>();
		fieldDef.add(new FieldDefinition("number", FieldDefinition.FIELD_TYPE.INTEGER, FieldDefinition.INDEX_TYPE.RANGE));
		test.register(tableName, fieldDef);
	}

	public String createInitialEntity(final Database test) {

		Map<String, Object> fields = new HashMap<>(2);
		fields.put("number", initialValue);
		return test.insert(tableName, fields);
	}

	public long transact(final Database test, final int threads, final String key, int repeat) {

		ScenarioExecutor<Void> executor = new ScenarioExecutor<Void>(threads);

		for (int n = 1; n <= threads; n++) {
			int delta = n % 2 == 1 ? 1 : -1;
			executor.addTask(new TransactTask(test, key, delta, repeat, true, tableName));
			System.out.println("Added task:" + n + " delta:" + delta);
		}
		long start = System.currentTimeMillis();
		executor.start();
		executor.getResults();
		return (System.currentTimeMillis() - start);
	}

	public static class TransactTask implements Callable<List<Void>> {

		public TransactTask(Database db, String key, int delta, int repeat, boolean printStatus, String tableName) {
			this.db = db;
			this.key = key;
			this.delta = delta;
			this.repeat = repeat;
			this.printStatus = printStatus;
			this.tableName = tableName;
		}

		private Database db;
		private String key;
		private int delta;
		private int repeat;
		private boolean printStatus;
		private String tableName;

		@Override
		public List<Void> call() throws Exception {
			long begin = System.currentTimeMillis();
			long start = System.currentTimeMillis();

			while (repeat != 0) {
				try {

					db.startTransaction();

					Map<String, Object> entity = db.get(key);

					if (entity != null) {

						int val = (int) entity.get("number");
						entity.put("number", val + delta);  // change by delta

						db.put(tableName, key, entity);

						db.commitTransaction();
						repeat--;
						System.out.println("Updated " + delta + " entity:" + key + " number:" + entity.get("number"));
					} else {
						System.out.println("transact " + Thread.currentThread().getName() + " Not found! key=" + key);
						db.rollbackTransaction();
					}
				} catch (RuntimeException re) {
//					re.printStackTrace();
					System.out.println("Collision: " + re.getMessage());
					db.rollbackTransaction();
				}
			}

			db.finish();
			return null;
		}
	}

}


