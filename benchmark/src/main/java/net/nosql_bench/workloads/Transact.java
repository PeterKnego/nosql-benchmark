package net.nosql_bench.workloads;

import net.nosql_bench.*;

import java.util.*;
import java.util.concurrent.Callable;

public class Transact implements Workload {

	private String tableName;

	@Override
	public void execute(Database test, Properties dbProperties, Properties workloadProperties) {

		tableName = workloadProperties.getProperty("tablename");
		boolean cleanup = Boolean.valueOf(workloadProperties.getProperty("cleanup", "false"));
		int threads = PropsUtil.expandInt(workloadProperties.getProperty("threads", "1"));

		int numEntities = PropsUtil.expandInt(workloadProperties.getProperty("transact.entities"));
		int ceiling = PropsUtil.expandInt(workloadProperties.getProperty("transact.ceiling"));
		if (ceiling % 2 != 0) {
			System.out.println("Error: property 'transact.ceiling' must be an even integer.");
		}

		setup(test, dbProperties);
		createInitialEntities(test, numEntities, ceiling);

		System.out.println("Starting transact..");
		long queryDuration = transact(test, threads, ceiling);
		System.out.println("Transact benchmark: entities=" + numEntities + " duration=" + queryDuration + " rate=" + ((1000 * numEntities) / queryDuration));

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

	public void createInitialEntities(final Database test, final int numEntities, final int ceiling) {

		for (int n = 1; n <= numEntities; n++) {
			Map<String, Object> fields = new HashMap<>(2);
			fields.put("number", 0);
			test.insert(tableName, fields);
		}

		for (int n = 1; n <= numEntities; n++) {
			Map<String, Object> fields = new HashMap<>(2);
			fields.put("number", ceiling);
			test.insert(tableName, fields);
		}
	}

	public long transact(final Database test, final int threads, final int ceiling) {

		ScenarioExecutor<Void> executor = new ScenarioExecutor<Void>(threads);

		for (int n = 1; n <= threads; n++) {
			executor.addTask(new TransactTask(test, ceiling, true, tableName));
		}
		long start = System.currentTimeMillis();
		executor.start();
		executor.getResults();
		return (System.currentTimeMillis() - start);
	}

	public static class TransactTask implements Callable<List<Void>> {

		public TransactTask(Database test, int ceiling, boolean printStatus, String tableName) {
			this.test = test;
			this.ceiling = ceiling;
			this.printStatus = printStatus;
			this.tableName = tableName;
		}

		private Database test;
		private int ceiling;
		private boolean printStatus;
		private String tableName;

		@Override
		public List<Void> call() throws Exception {
			long begin = System.currentTimeMillis();
			long start = System.currentTimeMillis();
			boolean loop = true;

			List<QueryPredicate> lessPredicate = new ArrayList<QueryPredicate>(1);
			lessPredicate.add(new QueryPredicate("number", QueryPredicate.OPERATOR.LESSER, ceiling / 2));

			List<QueryPredicate> greaterPredicate = new ArrayList<QueryPredicate>(1);
			greaterPredicate.add(new QueryPredicate("number", QueryPredicate.OPERATOR.GREATER, ceiling / 2));

			while (loop) {
				try {
					test.startTransaction();
					Map<String, Map<String, Object>> lesser = test.querySimple(tableName, lessPredicate, 0, 1);
					Map<String, Map<String, Object>> greater = test.querySimple(tableName, greaterPredicate, 0, 1);

					if (lesser.size() == 1 && greater.size() == 1) {

						Map<String, Object> lesserEnt = lesser.values().iterator().next();
						lesserEnt.put("number", ((Integer) lesserEnt.get("number")) + 1);  //increase by 1
						String lesserKey = lesser.keySet().iterator().next();

						Map<String, Object> greaterEnt = greater.values().iterator().next();
						greaterEnt.put("number", ((Integer) greaterEnt.get("number")) - 1);   // decrease by 1
						String greaterKey = greater.keySet().iterator().next();

						System.out.println("FOUND lesser:" + lesserKey + " greater:" + greaterKey);

						test.put(tableName, lesserKey, lesserEnt);
						test.put(tableName, greaterKey, greaterEnt);

						test.commitTransaction();
					} else {
						System.out.println("transact " + Thread.currentThread().getName() + " Not found! lesser:" + lesser.size() + " greater:" + greater.size());
						test.rollbackTransaction();
						loop = false;
					}
				} catch (RuntimeException re){
					System.out.println("Collision!");
					test.rollbackTransaction();
				} finally {
					test.finish();
				}

			}

			double duration = (System.currentTimeMillis() - start);
			if (printStatus) {
				System.out.println("transact " + Thread.currentThread().getName() + " dur:" + duration
						+ " time:" + (System.currentTimeMillis() - begin));
			}
			start = System.currentTimeMillis();

			return null;
		}
	}

}
