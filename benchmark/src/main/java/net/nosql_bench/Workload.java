package net.nosql_bench;

import java.util.Properties;

public interface Workload {

	void execute(DbTest test, Properties dbProperties, Properties workloadProperties);

}
