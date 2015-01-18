package net.nosql_bench.workloads;

import net.nosql_bench.*;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class BulkInsert implements Workload {

	public void execute(DbTest test, Properties dbProperties, Properties workloadProperties) {

		int count = PropsUtil.expandInt(workloadProperties.getProperty("count"));
		int threads = PropsUtil.expandInt(workloadProperties.getProperty("threads", "1"));

		setup(test, dbProperties, true);

		System.out.println("Starting inserts..");
		long insertDuration = insert(test, count, threads);
		System.out.println("Inserts benchmark: count=" + count + " duration=" + insertDuration + " rate=" + ((1000 * count) / insertDuration));

	}


	public static void setup(final DbTest test, Properties props, boolean dropExisting) {
		test.init(props);
		List<FieldDefinition> fieldDef = new ArrayList<>();
		fieldDef.add(new FieldDefinition("number", FieldDefinition.FIELD_TYPE.INTEGER, FieldDefinition.INDEX_TYPE.RANGE));
		fieldDef.add(new FieldDefinition("text", FieldDefinition.FIELD_TYPE.STRING, FieldDefinition.INDEX_TYPE.RANGE));

		test.register("BenchTest", fieldDef, dropExisting);
	}

	public static long insert(final DbTest test, final int count, final int threads) {

		ScenarioExecutor<Void> executor = new ScenarioExecutor<Void>(threads);

		for (int n = 1; n <= threads; n++) {
			executor.addTask(new InsertTask(test, count / threads, n == 1));
		}

		long start = System.currentTimeMillis();
		executor.start();
		executor.getResults();
		return (System.currentTimeMillis() - start);
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
				fields.put("text", Tester.randomWord());

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

}
