package jmxmetric;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;

public class JMXMetricProducer {
	final static Logger logger = Logger.getLogger(JMXMetricProducer.class);

	HashMap<ObjectName, String[]> objectNameToAttributeNameArray = new HashMap<ObjectName, String[]>();

	KafkaProducer<String, String> producer;

	JMXConnector jmxConnector;

	MBeanServerConnection mbeanConn;

	ExecutorService executorService;

	public void createProducer(String[] args) throws IOException,
			InstanceNotFoundException, IntrospectionException,
			ReflectionException, MalformedObjectNameException, MBeanException {
		if (args.length != 4) {
			throw new RuntimeException(
					args.length
							+ " arguments supplied. Should be 4. Usage: topicname jmxhost:port brokerhost:port sleeptimems"
							+ "\nExample usage: metricstopic localhost:9999 broker1:9092 5000");
		}

		String topic = args[0];
		String jmxHost = args[1];
		String brokerList = args[2];
		long sleepTime = Long.parseLong(args[3]);

		logger.info("Using Topic:" + topic + " jmxhost:" + jmxHost
				+ " brokerlist:" + brokerList + " sleeptimems:" + sleepTime);

		Properties props = createProperties(brokerList);
		producer = new KafkaProducer<String, String>(props);

		createJMXConnector(jmxHost);

		// Create a multi-threaded consumer
		executorService = Executors.newFixedThreadPool(1);

		executorService.submit(new JMXThread(objectNameToAttributeNameArray,
				producer, mbeanConn, topic, jmxHost, sleepTime));

		addShutdownHook();
	}

	private void addShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				logger.info("Shutting down");
				executorService.shutdown();

				try {
					jmxConnector.close();
				} catch (IOException e) {
					logger.error("Error in closing JMX", e);
				}
				
				producer.close();
				logger.info("Shut down");
			}
		});
	}

	private void createJMXConnector(String hostAndPort)
			throws MalformedURLException, IOException,
			MalformedObjectNameException, InstanceNotFoundException,
			IntrospectionException, ReflectionException {
		String url = "service:jmx:rmi:///jndi/rmi://" + hostAndPort + "/jmxrmi";
		JMXServiceURL serviceUrl = new JMXServiceURL(url);
		jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);

		mbeanConn = jmxConnector.getMBeanServerConnection();

		// Get the list of Kafka MBeans
		Set<ObjectName> names = mbeanConn.queryNames(
				new ObjectName("kafka*:*"), null);

		for (ObjectName name : names) {
			// Retrieve all of the attributes once
			MBeanInfo attribute = mbeanConn.getMBeanInfo(name);
			MBeanAttributeInfo[] attributeInfos = attribute.getAttributes();

			String[] attributeNames = new String[attributeInfos.length];

			for (int i = 0; i < attributeInfos.length; i++) {
				attributeNames[i] = attributeInfos[i].getName();
			}

			objectNameToAttributeNameArray.put(name, attributeNames);
		}
	}

	private Properties createProperties(String brokerList) {
		Properties props = new Properties();
		// Configure brokers to connect to
		props.put("bootstrap.servers", brokerList);
		// Configure serializer classes
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	public static void main(String[] args) {
		JMXMetricProducer metricProducer = new JMXMetricProducer();

		try {
			metricProducer.createProducer(args);
		} catch (Exception e) {
			logger.error("Error creating producer", e);
		}
	}
}
