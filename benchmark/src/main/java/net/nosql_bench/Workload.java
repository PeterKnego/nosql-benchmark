package net.nosql_bench;

import java.util.Properties;

public interface Workload {

	void execute(Database test, Properties dbProperties, Properties workloadProperties);

}
