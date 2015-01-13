package net.nosql_bench;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

	public static String dbClass = "net.nosql_bench.OrientDbTest";

	public static void main(String[] args) {
		if (args.length != 1 && args.length != 2 && args.length != 3) {
			System.out.println("Wrong arguments, must be: <orient|arango|aerospike> threads properties_path");
			return;
		}

		final String db = args[0];
		int threads = args.length >= 2 ? Integer.valueOf(args[1]) : 1;
		System.out.println("Working Directory = " + System.getProperty("user.dir"));

		final String propertiesPath = args.length == 3 ? args[2] : "./bench.properties";

		Properties props = new Properties();

		try {
			props.load(new FileInputStream(propertiesPath));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		DbTest test;
		if (db.equals("orientdb")) {
			try {
				test = (DbTest) Main.class.getClassLoader().loadClass(dbClass).newInstance();
			} catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
				e.printStackTrace();
				return;
			}
		} else {
			System.out.println("Unknown database: " + db);
			return;
		}

		int insertCount = 100_000;
		int queryCount = 100;
		threads = 4;

		setup(test, props);
		System.out.println("Starting inserts..");
		long insertDuration = insert(test, insertCount, threads);
		System.out.println("Starting queries..");
		long queryDuration = query(test, queryCount, threads);

		System.out.println("Inserts bench: count=" + insertCount + " duration=" + insertDuration + " rate=" + (insertCount / insertDuration));
		System.out.println("Queries bench: count=" + queryCount + " duration=" + queryDuration + " rate=" + (queryCount / queryDuration));
	}

	public static void setup(final DbTest test, Properties props) {
		test.init(props);
		List<FieldDefinition> fieldDef = new ArrayList<>();
		fieldDef.add(new FieldDefinition("number", FieldDefinition.FIELD_TYPE.LONG, FieldDefinition.INDEX_TYPE.RANGE));
		fieldDef.add(new FieldDefinition("text", FieldDefinition.FIELD_TYPE.STRING, FieldDefinition.INDEX_TYPE.FULLTEXT));

		test.register("BenchTest", fieldDef);
	}

	public static long insert(final DbTest test, final int count, final int threads) {

		ScenarioExecutor<Void> executor = new ScenarioExecutor<Void>(threads);

		for (int n = 1; n <= threads; n++) {
			executor.addTask(new InsertTask(test, count / threads, n == 1));
		}

		long start = System.currentTimeMillis();
		executor.start();
		executor.getResults();
		return (System.currentTimeMillis() - start) / 1000;
	}

	public static class InsertTask implements Callable<List<Void>> {

		public InsertTask(DbTest test, int countInThread, boolean printStatus) {
			this.test = test;
			this.countInThread = countInThread;
			this.printStatus = printStatus;
		}

		private static AtomicInteger totalCount = new AtomicInteger(0);

		private int countInThread;
		private boolean printStatus;
		private DbTest test;

		@Override
		public List<Void> call() throws Exception {

			long start = System.currentTimeMillis() - 1;
			double curTime = start;

			for (int i = 0; i < countInThread; i++) {

				Map<String, Object> fields = new HashMap<>(2);
				fields.put("number", Tester.randomInt());
				fields.put("text", Tester.randomWord() + " " + Tester.randomWord() + " " + Tester.randomWord());

				test.insert("BenchTest", fields);
				int total = totalCount.addAndGet(1);

				if (printStatus && i % 1000 == 0) {
					double duration = (System.currentTimeMillis() - start) / 1000;
					double lastDur = (System.currentTimeMillis() - curTime) / 1000.0;
					curTime = System.currentTimeMillis();
					System.out.println("insert " + Thread.currentThread().getName() + " " + total + " dur:" + duration + "  avg_rate:" + (int) (total / duration) + "  cur_rate:" + (int) (1000.0 / lastDur));
				}
			}

			return null;
		}
	}

	public static long query(final DbTest test, final int count, final int threads) {

		ScenarioExecutor<Void> executor = new ScenarioExecutor<Void>(threads);

		for (int n = 1; n <= threads; n++) {
			executor.addTask(new QueryTask(test, count / threads, true));
		}
		long start = System.currentTimeMillis();
		executor.start();
		executor.getResults();
		return (System.currentTimeMillis() - start) / 1000;
	}

	public static class QueryTask implements Callable<List<Void>> {

		public QueryTask(DbTest test, int countInThread, boolean printStatus) {
			this.test = test;
			this.countInThread = countInThread;
			this.printStatus = printStatus;
		}

		private static AtomicInteger totalCount = new AtomicInteger(0);

		private int countInThread;
		private boolean printStatus;
		private DbTest test;

		@Override
		public List<Void> call() throws Exception {
			long start = System.currentTimeMillis();
			for (int i = 0; i < countInThread; i++) {
				List<QueryPredicate> predicates = new ArrayList<QueryPredicate>(1);
				predicates.add(new QueryPredicate("text", QueryPredicate.OPERATOR.LIKE, Tester.randomWord()));
				int res = test.querySimple("BenchTest", predicates);
				double duration = (System.currentTimeMillis() - start) / 1000;
				if (printStatus) {
					System.out.println("query " + Thread.currentThread().getName() + " " + i + " dur:" + duration + " results:" + res);
				}
				start = System.currentTimeMillis();
			}
			return null;
		}
	}

}
