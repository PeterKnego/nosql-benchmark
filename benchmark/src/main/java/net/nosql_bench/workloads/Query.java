package net.nosql_bench.workloads;

import net.nosql_bench.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class Query implements Workload {

	public void execute(DbTest test, Properties dbProperties, Properties workloadProperties) {

		int count = PropsUtil.expandInt(workloadProperties.getProperty("count"));
		int threads = PropsUtil.expandInt(workloadProperties.getProperty("threads", "1"));

		setup(test, dbProperties, true);

		System.out.println("Starting inserts..");
		long queryDuration = query(test, count, threads);
		System.out.println("Query benchmark: count=" + count + " duration=" + queryDuration + " rate=" + ((1000 * count) / queryDuration));
	}


	public static void setup(final DbTest test, Properties props, boolean dropExisting) {
		test.init(props);
		List<FieldDefinition> fieldDef = new ArrayList<>();
		fieldDef.add(new FieldDefinition("number", FieldDefinition.FIELD_TYPE.INTEGER, FieldDefinition.INDEX_TYPE.RANGE));
		fieldDef.add(new FieldDefinition("text", FieldDefinition.FIELD_TYPE.STRING, FieldDefinition.INDEX_TYPE.RANGE));

		test.register("BenchTest", fieldDef, dropExisting);
	}

	public static long query(final DbTest test, final int count, final int threads) {

		ScenarioExecutor<Void> executor = new ScenarioExecutor<Void>(threads);

		for (int n = 1; n <= threads; n++) {
			executor.addTask(new QueryTask(test, count / threads, true));
		}
		long start = System.currentTimeMillis();
		executor.start();
		executor.getResults();
		return (System.currentTimeMillis() - start);
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
			long begin = System.currentTimeMillis();
			long start = System.currentTimeMillis();
			for (int i = 0; i < countInThread; i++) {
				List<QueryPredicate> predicates = new ArrayList<QueryPredicate>(1);
				predicates.add(new QueryPredicate("text", QueryPredicate.OPERATOR.EQUALS, Tester.randomWord()));
				int res = test.querySimple("BenchTest", predicates);
				double duration = (System.currentTimeMillis() - start);
				if (printStatus) {
					System.out.println("query " + Thread.currentThread().getName() + " " + i + " dur:" + duration
							+ " time:" + (System.currentTimeMillis() - begin) + " results:" + res);
				}
				start = System.currentTimeMillis();
			}
			return null;
		}
	}
}
