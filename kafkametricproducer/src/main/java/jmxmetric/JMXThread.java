package jmxmetric;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

public class JMXThread extends Thread {
	final static Logger logger = Logger.getLogger(JMXThread.class);

	HashMap<ObjectName, String[]> objectNameToAttributeNameArray = new HashMap<ObjectName, String[]>();

	KafkaProducer<String, String> producer;

	MBeanServerConnection mbeanConn;

	String topic;

	String host;

	long sleepTime;

	public JMXThread(
			HashMap<ObjectName, String[]> objectNameToAttributeNameArray,
			KafkaProducer<String, String> producer,
			MBeanServerConnection mbeanConn, String topic, String host,
			long sleepTime) {
		super();
		this.objectNameToAttributeNameArray = objectNameToAttributeNameArray;
		this.producer = producer;
		this.mbeanConn = mbeanConn;
		this.topic = topic;
		this.host = host;
		this.sleepTime = sleepTime;
	}

	@Override
	public void run() {
		logger.debug("Running producer thread");

		while (true) {
			try {
				String json = getJMX();

				produce(json);

				Thread.sleep(sleepTime);
			} catch (Exception e) {
				logger.error("Error in JMXThread", e);
			}

		}
	}

	private String getJMX() throws InstanceNotFoundException,
			ReflectionException, IOException {
		StringBuilder builder = new StringBuilder(" { ");

		Iterator<Entry<ObjectName, String[]>> iterator = objectNameToAttributeNameArray
				.entrySet().iterator();

		while (iterator.hasNext()) {
			Entry<ObjectName, String[]> entry = iterator.next();

			processObjectName(builder, iterator, entry);
		}

		builder.append(" } ");

		return builder.toString();
	}

	private void processObjectName(StringBuilder builder,
			Iterator<Entry<ObjectName, String[]>> iterator,
			Entry<ObjectName, String[]> entry)
			throws InstanceNotFoundException, ReflectionException, IOException {
		builder.append(" \"" + entry.getKey().getCanonicalName() + "\" : {");

		AttributeList list = mbeanConn.getAttributes(entry.getKey(),
				entry.getValue());
		Iterator<Object> attributeIterator = list.iterator();

		while (attributeIterator.hasNext()) {
			Attribute attribute = (Attribute) attributeIterator.next();

			processAttribute(builder, attributeIterator, attribute);
		}

		builder.append(" }");

		if (iterator.hasNext()) {
			builder.append(",");
		}
	}

	private void processAttribute(StringBuilder builder,
			Iterator<Object> attributeIterator, Attribute attribute) {
		builder.append(" \"" + attribute.getName() + "\": ");

		// Only add quotes around non-numeric
		if (attribute.getValue() instanceof Integer
				|| attribute.getValue() instanceof Long) {
			builder.append(attribute.getValue());
			// Check Floats and Doubles for NaNs
		} else if (attribute.getValue() instanceof Double) {
			if (((Double) attribute.getValue()).isNaN()
					|| ((Double) attribute.getValue()).isInfinite()) {
				builder.append(0);
			} else {
				builder.append(attribute.getValue());
			}
		} else if (attribute.getValue() instanceof Float) {
			if (((Float) attribute.getValue()).isNaN()
					|| ((Float) attribute.getValue()).isInfinite()) {
				builder.append(0);
			} else {
				builder.append(attribute.getValue());
			}
		} else {
			builder.append("\"" + attribute.getValue() + "\"");
		}

		if (attributeIterator.hasNext()) {
			builder.append(",");
		}
	}

	private void produce(String json) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(
				topic, host, json);
		producer.send(record);

		logger.debug("Produced");
	}
}
