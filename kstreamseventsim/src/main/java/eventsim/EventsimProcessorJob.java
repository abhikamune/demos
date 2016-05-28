package eventsim;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStoreSupplier;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.MinMaxPriorityQueue;

import scala.Tuple2;

public class EventsimProcessorJob {
	static int windowsizems = 0;
	static int maximumGenerations = 0;

	final static Logger logger = Logger.getLogger(EventsimProcessorJob.class);

	private static class EventsimProcessorDef implements ProcessorSupplier<String, String> {

		@Override
		public Processor<String, String> get() {
			return new Processor<String, String>() {
				private ProcessorContext context;
				private InMemoryKeyValueStoreSupplier<String, String> kvStore;
				private KeyValueStore<String, String> stateStore;
				
				ObjectMapper mapper;

				private HashMap<String, Long> artistToPlayCount;
				private HashMap<String, Long> songToPlayCount;

				private MinMaxPriorityQueue<Tuple2<String, Long>> artistToPlayCountQueue;
				private MinMaxPriorityQueue<Tuple2<String, Long>> songToPlayCountQueue;

				private static final int TOP_N = 10;

				private AtomicInteger currentGeneration = new AtomicInteger(0);

				Serde<String> stringSerde = Serdes.String();
				
				@Override
				public void init(ProcessorContext context) {
					this.context = context;
					this.context.schedule(windowsizems);
					this.kvStore = new InMemoryKeyValueStoreSupplier<String, String>(
							"local-state", stringSerde, stringSerde);
					this.stateStore = (KeyValueStore<String, String>) this.kvStore.get();
					this.mapper = new ObjectMapper();

					artistToPlayCount = new HashMap<String, Long>();
					songToPlayCount = new HashMap<String, Long>();

					createQueues();
				}

				private void createQueues() {
					Comparator<Tuple2<String, Long>> comparator = new Comparator<Tuple2<String, Long>>() {

						@Override
						public int compare(Tuple2<String, Long> o1,
								Tuple2<String, Long> o2) {
							return o1._2().compareTo(o2._2()) * -1;
						}

					};

					artistToPlayCountQueue = MinMaxPriorityQueue
							.orderedBy(comparator).maximumSize(TOP_N).create();
					songToPlayCountQueue = MinMaxPriorityQueue
							.orderedBy(comparator).maximumSize(TOP_N).create();
				}

				@Override
				public void process(String key, String value) {
					String mapKey = getKeyName(value);
					String oldValue = stateStore.get(mapKey);

					if (oldValue == null) {
						// Swap k/v around as eventsim key is null
						stateStore.put(mapKey, value);
					} else {
						// TODO: Handle when k/v already there
						// this.kvStore.put(key, oldValue + newValue);
					}

					context.commit();
				}

				@Override
				public void punctuate(long streamTime) {
					currentGeneration.incrementAndGet();

					KeyValueIterator<String, String> iter = stateStore.all();

					double totalDuration = 0;

					long totalEntries = 0;

					while (iter.hasNext()) {
						KeyValue<String, String> keyValue = iter.next();

						totalEntries++;

						if (keyValue.value != null) {
							try {
								JsonNode rootNode = mapper.readTree(
										keyValue.value);
								/*
								 * Example input:
								 * {"ts":1442428043000,"userId":23545,
								 * "sessionId":23544
								 * ,"page":"NextSong","auth":"Logged In"
								 * ,"method":"PUT"
								 * ,"status":200,"level":"paid","itemInSession"
								 * :35,"location"
								 * :"New York-Newark-Jersey City, NY-NJ-PA"
								 * ,"userAgent":
								 * "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36\""
								 * ,"lastName":"Barnes","firstName":"Camila",
								 * "registration"
								 * :1442043066000,"gender":"F","artist"
								 * :"Radiohead","song"
								 * :"Creep (Explicit)","duration":235.7024}
								 */
								JsonNode artist = rootNode.path("artist");
								JsonNode song = rootNode.path("song");
								JsonNode duration = rootNode.path("duration");

								addOrUpdate(artist.asText(), artistToPlayCount);
								addOrUpdate(song.asText(), songToPlayCount);

								totalDuration += duration.asDouble();

								if (checkDelete(keyValue.key)) {
									iter.remove();
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}

					iter.close();

					context.forward(null, output(totalDuration, totalEntries));

					// Clear things for the next window
					artistToPlayCount.clear();
					songToPlayCount.clear();
				}

				private boolean checkDelete(String key) {
					// Use a rolling window and generations to keep more data
					String[] parts = key.split("-");

					try {
						int gen = Integer.parseInt(parts[1]);

						if (gen < currentGeneration.get() - maximumGenerations) {
							return true;
						}
					} catch (Exception e) {
						logger.debug(e);
					}
					
					return false;
				}

				private String output(double totalDuration, long totalEntries) {
					StringBuilder builder = new StringBuilder("{");

					// The total amount played
					builder.append("\"totalduration\":").append(totalDuration)
							.append(",");

					// Add the artist plays
					builder.append("\"artisttoplaycount\": [");
					processCounts(builder, artistToPlayCount,
							artistToPlayCountQueue);
					builder.append("], ");

					// Add the song plays
					builder.append("\"songtoplaycount\": [");
					processCounts(builder, songToPlayCount,
							songToPlayCountQueue);
					builder.append("],");

					// Add the other totals
					builder.append("\"totals\": {");
					builder.append("\"totalentries\": ").append(totalEntries)
							.append(",");
					builder.append("\"totalsongs\": ")
							.append(songToPlayCount.size()).append(",");
					builder.append("\"totalartists\": ").append(
							artistToPlayCount.size());
					builder.append("} }");

					return builder.toString();
				}

				private void processCounts(StringBuilder builder,
						HashMap<String, Long> map,
						MinMaxPriorityQueue<Tuple2<String, Long>> queue) {
					Iterator<java.util.Map.Entry<String, Long>> iterator = map
							.entrySet().iterator();

					queue.clear();

					while (iterator.hasNext()) {
						java.util.Map.Entry<String, Long> entry = iterator
								.next();
						queue.add(new Tuple2<String, Long>(entry.getKey(), entry
								.getValue()));
					}

					Iterator<Tuple2<String, Long>> queueIter = queue.iterator();

					while (queueIter.hasNext()) {
						Tuple2<String, Long> entry = queueIter.next();

						builder.append("{\"name\" : \"").append(entry._1())
								.append("\",");

						builder.append("\"count\" : ").append(entry._2())
								.append("}");

						if (queueIter.hasNext()) {
							builder.append(",");
						}
					}
				}

				private void addOrUpdate(String key, HashMap<String, Long> map) {
					// Make sure the key isn't an empty string
					if (key.equals("")) {
						return;
					} else {
						Long currentValue = map.get(key);
						if (currentValue == null) {
							map.put(key, 1L);
						} else {
							map.put(key, currentValue + 1L);
						}
					}
				}

				@Override
				public void close() {
					this.kvStore.get().close();
				}

				private String getKeyName(String value) {
					return String.valueOf(value.hashCode()) + "-"
							+ currentGeneration.get();
				}
			};
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new RuntimeException(
					args.length
							+ " arguments supplied. Should be 1. Usage: windowsizems maxgenerations"
							+ "\nExample usage: 30000 20");
		}

		windowsizems = Integer.parseInt(args[0]);
		maximumGenerations = Integer.parseInt(args[1]);

		logger.info("Using windowsizems:" + windowsizems + " maxgenerations:"
				+ maximumGenerations);

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-sim-processor");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,
				StringSerializer.class);
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,
				StringSerializer.class);
		props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
				WallclockTimestampExtractor.class);

		TopologyBuilder builder = new TopologyBuilder();

		builder.addSource("SOURCE", new StringDeserializer(),
				new StringDeserializer(), "eventsim");

		builder.addProcessor("PROCESS", new EventsimProcessorDef(), "SOURCE");

		builder.addSink("SINK", "eventsimstream", new StringSerializer(),
				new StringSerializer(), "PROCESS");

		KafkaStreams streaming = new KafkaStreams(builder, props);
		streaming.start();
	}
}
