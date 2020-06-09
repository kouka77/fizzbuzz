package io.barac;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PhoenixFragmentBolt extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String s,time,action;

	Connection connection;
	Statement statement;
	private Fields fields;

	public PhoenixFragmentBolt() {
		fields = new Fields("time","action");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// System.out.println(" try loading driver");
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			// below is the cnx string for production
			// connection =
			// DriverManager.getConnection("jdbc:phoenix:10.0.0.35,10.0.0.36,10.0.0.37,10.0.0.38:2181:/hbase-unsecure");

			// below is the cnx string fot Dev
			connection = DriverManager
					.getConnection("jdbc:phoenix:datanode.europe-west2-c.c.centered-accord-266509.internal:2181:/hbase-unsecure");

			statement = connection.createStatement();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		time = input.getString(0);
		action = input.getString(1);

		String query = "upsert into Tuto(time, action) values( '"
				+time +"','"+action+ "')";
		try {
		        System.out.println(query);
			statement.executeUpdate(query);
			connection.commit();
			collector.emit(new Values(time, action));
			

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void cleanup() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(fields);
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
