import requests
import aiohttp, asyncio
import json
from confluent_kafka import Producer


def initiate_producer():
    prd_config = {
        'bootstrap.servers': 'localhost:9092'
    }
    prd = Producer(prd_config)
    return prd


def pproduce(val, producer, topic):
    encodedvalue = json.dumps(val).encode('utf-8')
    producer.produce(topic=topic, value=encodedvalue)
    producer.flush()


def report(err, msg):
    if err:
        return f"ERROR: {err}"
    else:
        return f"{msg.value().decode('utf-8')}"


def get_status():
    return 200


async def f(seconds):
    values = []
    producer = initiate_producer()
    headers = {
        'User-Agent': 'test-test/1.0 (test@test.test)'
    }
    t = ""
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get("https://stream.wikimedia.org/v2/stream/recentchange") as response:
            content = ""
            status = response.status
            async for piece in response.content.iter_any():
                content += piece.decode('utf-8')
                if 'event: message' not in content:
                    continue
                chunks = content.split("event: message")
                content = chunks[-1]
                chunks = chunks[:-1]
                chunks = chunks[1:] if (chunks and chunks[0].strip() == "") else chunks
                n = len(chunks)
                for i in range(n):
                    chunk = chunks[i]
                    if "id: " not in chunk or "data: " not in chunk:
                        continue
                    id_part = chunk.split("id: ", 1)[1].split("\n", 1)[0].strip()
                    data_part = chunk.split("data: ", 1)[1].split("\n", 1)[0].strip()
                    id_part_asjson = json.loads(id_part)
                    data_part_asjson = json.loads(data_part)
                    meta_withindata = data_part_asjson["meta"]
                    id_part1 = id_part_asjson[0]
                    topic = id_part1["topic"]
                    t = topic
                    partition = id_part1["partition"]
                    timestamp = id_part1["timestamp"]
                    offset = meta_withindata["offset"]
                    user = data_part_asjson["user"]
                    is_bot = data_part_asjson["bot"]
                    date, time1 = meta_withindata["dt"].split("T")
                    time, a = time1.split(".")
                    values.append({
                        "topic": topic,
                        "partition": partition,
                        "timestamp": timestamp,
                        "offset": offset,
                        "date": date,
                        "time": time,
                        "user": user,
                        "is_bot": is_bot,
                        "title": data_part_asjson["title"],
                        "wiki": data_part_asjson["wiki"],
                        "namespace": data_part_asjson["namespace"],
                        "type": data_part_asjson["type"],
                        "comment": data_part_asjson.get("comment", ""),
                        "server_name": data_part_asjson["server_name"]
                    })
                    pproduce(values[-1], producer, topic)
                    if seconds != 0:
                        await asyncio.sleep(seconds)
    return values, t, status


async def r():
    z = await f(0)


if __name__ == "__main__":
    asyncio.run(r())