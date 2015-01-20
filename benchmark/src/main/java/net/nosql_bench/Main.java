package net.nosql_bench;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {
	public static void main(String[] args) {

		if (args.length != 2) {
			System.out.println("Wrong arguments, must be: db_name_or_properties_path workload_name_or_properties_path");
			return;
		}

		System.out.println("Working Directory = " + System.getProperty("user.dir"));

		final String dbPropertiesPath = args[0];
		final String workloadPropertiesPath = args[1];

		Properties workloadProperties = new Properties();
		try {
			InputStream propertiesStream = Main.class.getResourceAsStream("/workloads/" + workloadPropertiesPath + ".properties");
			if (propertiesStream == null) {
				propertiesStream = new FileInputStream(workloadPropertiesPath);
			}
			workloadProperties.load(propertiesStream);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		Properties dbProperties = new Properties();
		try {
			InputStream propertiesStream = Main.class.getResourceAsStream("/" + dbPropertiesPath + ".properties");
			if (propertiesStream == null) {
				propertiesStream = new FileInputStream(dbPropertiesPath);
			}
			dbProperties.load(propertiesStream);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		Workload workload;
		try {
			String className = workloadProperties.getProperty("class");
			if (className == null) {
				System.out.println("Error: property 'class' not defined in workload.properties ");
				return;
			}
			workload = (Workload) Main.class.getClassLoader().loadClass(className).newInstance();

		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			System.out.println(e.getMessage());
			return;
		}

		DbTest test;
		try {
			String className = dbProperties.getProperty("class");
			if (className == null) {
				System.out.println("Error: property 'class' not defined in database properties: " + dbPropertiesPath);
				return;
			}
			test = (DbTest) Main.class.getClassLoader().loadClass(className).newInstance();
		} catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
			System.out.println(e.getMessage());
			return;
		}

		workload.execute(test, dbProperties, workloadProperties);
	}
}
