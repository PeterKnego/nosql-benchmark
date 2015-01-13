package net.nosql_bench;

import java.util.*;
import java.util.concurrent.*;

/**
 * A convenience class that wraps Executor for simpler use.
 * <p/>
 * Usage scenario:
 * <ol>
 * <li>Instantiate ScenarioExecutor</li>
 * <li>Call multiple times {@link #addTask(java.util.concurrent.Callable)} passing your implementation of {@link java.util.concurrent.Callable}</li>
 * <li>Call {@link #start()} to start execution. After this tasks can not be added anymore.</li>
 * <li>Call {@link #getResults()} which will wait for all tasks to finish and gather all results in a set.</li>
 * </ol>
 *
 * @param <T>
 */
public class ScenarioExecutor<T> {

	private final ExecutorService es;
	private Collection<Callable<List<T>>> todo = new ArrayList<Callable<List<T>>>();
	private List<Future<List<T>>> results;

	/**
	 * @param maxThreads Number of threads in the internal thread pool. Max number is 50 per current request, limited by current GAE implementation.
	 */
	public ScenarioExecutor(int maxThreads) {
		if (maxThreads > 50) {
			throw new IllegalArgumentException("GAE supports max 50 threads per request");
		}
		es = Executors.newFixedThreadPool(maxThreads);
	}

	/**
	 * Starts the execution of tasks. After this method is called, new tasks can not be added anymore.
	 */
	public void start() {
		try {
			results = es.invokeAll(todo);
		} catch (InterruptedException e) {
			// just tasks cancelled
		}
		es.shutdown(); // will shutdown after all tasks are finished
	}

	/**
	 * Attempts to stop all actively executing tasks, halts the processing of waiting tasks.
	 */
	public void cancel() {
		es.shutdownNow();
	}

	/**
	 * Add tasks to queue. Can be called many times. Must be called before {@link #start()}.
	 *
	 * @param task An implementation of {@link Callable}
	 */
	public void addTask(Callable<List<T>> task) {
		if (es.isShutdown() || es.isTerminated()) {
			throw new IllegalStateException("Cannot add tasks after start() is called!");
		}
		todo.add(task);
	}

	/**
	 * A blocking method that waits for all task to finish executing and returns an aggregate set of results.
	 * Set requires elements to be unique, so watch out when implementing T's identity methods: hashCode() and equals().
	 *
	 * @return The result set
	 */
	public Set<T> getResults() {
		Set<T> allResults = new HashSet<T>(results.size());
		for (Future<List<T>> result : results) {
			List<T> partResults = null;
			try {
				partResults = result.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			}
			if (partResults != null) {
				allResults.addAll(partResults);
			}
		}
		return allResults;
	}
}
