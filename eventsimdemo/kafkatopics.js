kafkatopics = [
  {
    "description": "Broker Byte Metrics",
    "topics" : [
      {
        "topic": "kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics",
        "values": ["OneMinuteRate"],
        "description": "Broker BytesInPerSec"
      },
      {
        "topic": "kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics",
        "values": ["OneMinuteRate"],
        "description": "Broker BytesOutPerSec"
      }
    ]
  },
  {
    "description": "Broker Message Metrics",
    "topics" : [
      {
        "topic": "kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics",
        "values": ["OneMinuteRate"],
        "description": "Broker MessagesInPerSec"
      }
    ]
  },
  {
    "description": "Produce Latency",
    "topics": [
      {
        "topic" : "kafka.network:name=TotalTimeMs,request=Produce,type=RequestMetrics",
        "values": ["Mean"],
        "description": "Produce TotalTimeMs"
      }
    ]
  },
  {
    "description": "Consumer Latency",
    "topics": [
      {
        "topic" : "kafka.network:name=TotalTimeMs,request=FetchConsumer,type=RequestMetrics",
        "values": ["Mean"],
        "description": "Consumer Latency"
      }
    ]
  }
];
