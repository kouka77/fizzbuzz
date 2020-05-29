package io.barac;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class Topology {
	private static String zkhost, inputTopic, KafkaBroker, consumerGroup, topologyName, numWorkersString, debug,
			spoutId, soketIOServerURL, socketIOSecret, blockIPDuration, jweKey;
	public static String phoenixStringConnexion;
	private static int numWorkers;
	private static int Max_Spout_Pending ;
	// these are columns of hbase
	static List<String> zklist;
	private static int numSpout, numConverter, numPrediction, numFilter, numHbase, numTaskSpout, numTasksPrediction,
			numTaskConverter, numTaskHbase, numTaskFilter, scoreAttack, fetchSizeBytes, socketTimeoutMs,
			offsetCommitPeriodMs, maxUncommittedOffsets, pollTimeoutMs;

	// private static String metaStoreURI, dbName, tblName;
	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(Topology.class);

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();
		// getting properties from topology.properties
		Properties prop = new Properties();
		InputStream input = null;
		input = new FileInputStream(args[0]);
		prop.load(input);
		spoutId = prop.getProperty("spoutId");
		topologyName = prop.getProperty("topologyName");
		zkhost = prop.getProperty("zkhost");
		inputTopic = prop.getProperty("inputTopic");
		// outputTopic = prop.getProperty("outputTopic");
		KafkaBroker = prop.getProperty("KafkaBroker");
		consumerGroup = prop.getProperty("consumerGroup");
		numWorkersString = prop.getProperty("numWorkers");
		Max_Spout_Pending = Integer.parseInt(prop.getProperty("Max_Spout_Pending"));
		soketIOServerURL = prop.getProperty("soketIOServerURL");
		socketIOSecret = prop.getProperty("socketIOSecret");
		blockIPDuration = prop.getProperty("blockIPDuration");
		debug = prop.getProperty("debug");
		jweKey = prop.getProperty("jweKey");
		// setting workers for each bolt from properties
		numSpout = Integer.parseInt(prop.getProperty("numSpout"));
		numConverter = Integer.parseInt(prop.getProperty("numConverter"));
		numPrediction = Integer.parseInt(prop.getProperty("numPrediction"));
		numFilter = Integer.parseInt(prop.getProperty("numFilter"));
		numHbase = Integer.parseInt(prop.getProperty("numHbase"));
		numTaskSpout = Integer.parseInt(prop.getProperty("numTaskSpout"));
		numTasksPrediction = Integer.parseInt(prop.getProperty("numTasksPrediction"));
		numTaskConverter = Integer.parseInt(prop.getProperty("numTaskConverter"));
		numTaskFilter = Integer.parseInt(prop.getProperty("numTaskFilter"));
		numTaskHbase = Integer.parseInt(prop.getProperty("numTaskHbase"));
		scoreAttack = Integer.parseInt(prop.getProperty("scoreAttack"));
		socketTimeoutMs = Integer.parseInt(prop.getProperty("socketTimeoutMs"));
		fetchSizeBytes = Integer.parseInt(prop.getProperty("fetchSizeBytes"));
		offsetCommitPeriodMs = Integer.parseInt(prop.getProperty("offsetCommitPeriodMs"));
		maxUncommittedOffsets = Integer.parseInt(prop.getProperty("maxUncommittedOffsets"));
		pollTimeoutMs = Integer.parseInt(prop.getProperty("pollTimeoutMs"));
		phoenixStringConnexion = prop.getProperty("PhoenixStringConnexion");
		boolean b = Boolean.parseBoolean(debug);
		BrokerHosts hosts = new ZkHosts(zkhost);

		SpoutConfig spoutConfig = new SpoutConfig(hosts, inputTopic, "/" + KafkaBroker, consumerGroup);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		spoutConfig.maxOffsetBehind = Long.MAX_VALUE;
		spoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
		spoutConfig.ignoreZkOffsets = true;
		spoutConfig.zkPort = 2181;
		spoutConfig.socketTimeoutMs = socketTimeoutMs;
		spoutConfig.fetchSizeBytes = fetchSizeBytes;

		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		builder.setSpout(spoutId, kafkaSpout, numSpout).setNumTasks(numTaskSpout)
				.addConfiguration("offset.commit.period.ms", offsetCommitPeriodMs)
				.addConfiguration("max.uncommitted.offsets", maxUncommittedOffsets)
				.addConfiguration("poll.timeout.ms", pollTimeoutMs);
 
		builder.setBolt("Converter", new Converter(), numConverter).shuffleGrouping(spoutId)
		.setNumTasks(numTaskConverter);
		
		builder.setBolt("Converter2", new Converter2(), numHbase).shuffleGrouping("Converter")
		.setNumTasks(numTaskHbase);
		
		
		
		
		/*
		 * bellow is the creation code of prediction bolt uncomment to activate
		 */
		
		/*
		 * bellow is the creation code of packetfilter uncomment to activate
		 */

		// builder.setBolt("PacketFilter", new PacketFilter(),
		// numFilter).shuffleGrouping("predictions", "packet")
		// .setNumTasks(numTaskFilter);

		
		/*
		 * bellow is the creation code of fragment filter and fragment dump into phoenix
		 * uncomment to activate
		 */

		

		
		conf.setDebug(false);


		if (args != null && args.length > 0) {
			if (numWorkersString.length() > 0) {
				numWorkers = Integer.parseInt(numWorkersString);
			}
			conf.setNumWorkers(numWorkers);
			conf.setMaxSpoutPending(Max_Spout_Pending);
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		

		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

	public static String getPhoenixStringConnexion() {
		return phoenixStringConnexion;
	}

	public static void setPhoenixStringConnexion(String phoenixStringConnexion) {
		Topology.phoenixStringConnexion = phoenixStringConnexion;
	}
}
