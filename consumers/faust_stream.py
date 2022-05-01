"""Defines trends calculations for stations"""
import json
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("kafka-connect-raw-stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

table = app.Table(
   "stations-steam-summary",
   default=int,
   partitions=1,
   changelog_topic=out_topic,
)

@app.agent(topic)
async def station(stations):

    async for s in stations:
        colour = "N/A"
        if s.red:
            colour="red"
        elif s.blue:
            colour="blue"
        elif s.green:
            colour="green"

        table[s.station_id] = TransformedStation(
            station_id=s.station_id,
            station_name=s.station_name,
            order=s.order,
            line=colour
        )
        print(f"Station ID: {s.station_id}, Data: {table[s.station_id]}")

if __name__ == "__main__":
    app.main()
