# demos

Stream processing demo showing how Kafka can be used to create a real-time dashboard at scale.

## Scenario

We want a dashboard presenting Big Data in real-time. Our company is a Spotify-esque service that lets users play songs. We want to see the real-time statistics and usage of our 10 million subscribers. Our dashboard is showing how many people are using our free or paid service. We see which pages they're hitting and the location in the United States where the person is.

We also want to see which artists and songs are trending. We're using Kafka Streams to aggregate artist and song plays over time. We want to see how much our service is being used, so we're showing the total song time played by our users.

This dashboard lets us see our KPI (Key Performance Indicators) in real-time and at Big Data scale.

## Technical details

The initial data comes from [Eventsim](https://github.com/Interana/eventsim). It's designed to replicate page requests for a fake music web site (picture something like Spotify); the results look like real use data, but are totally fake. All Eventsim data is published to Kafka.

Kafka Streams then processes the Eventsim topic and creates three rolling aggregations on the data. One aggregation is number of songs played by an artist in the time window. Another aggregation is number of songs broken down by the song itself in the time window. The final aggregation is the total song time played in the time window.

A producer runs and retrieves Kafka metrics data over JMX. This data is then produced to a metrics topic.

Finally, all data is visualized on the page. The web page is making direct AJAX calls to the Kafka REST interface. There are no custom servlets. The data is retrieved using JavaScript intervals and visualized with D3.js.
