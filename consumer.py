import json
from confluent_kafka import Consumer


def bot_tracker(event, cns_config):
    is_bot_count = 0
    is_not_bot_count = 0
    cns_config['group.id'] = 'bots-numbers'
    if event.get("is_bot"):
        is_bot_count += 1
    else:
        is_not_bot_count += 1
    return is_bot_count, is_not_bot_count


def events_wiki(event, cns_config, wiki, count, i):
    cns_config['group.id'] = 'events-wiki'

    if (len(wiki) == 0) or (event.get("wiki") not in wiki):
        wiki.append(event.get("wiki"))
        count.append(1)
    elif event.get("wiki") in wiki:
        i = wiki.index(event.get("wiki"))
        count[i] += 1

    events_wiki_count = {
        'event_wiki': event.get("wiki"),
        'count: ': count[i]
    }

    return events_wiki_count, wiki, count, i


def events_title(event, cns_config, titles, count_title, j):
    cns_config['group.id'] = 'events-title'

    if (len(titles) == 0) or (event.get("title") not in titles):
        titles.append(event.get("title"))
        count_title.append(1)
    elif event.get("title") in titles:
        j = titles.index(event.get("title"))
        count_title[j] += 1

    events_title_count = {
        'event_title': event.get("title"),
        'count: ': count_title[j]
    }

    return events_title_count, titles, count_title, j


def consumer():
    cns_config = {
        'bootstrap.servers': 'localhost:9092',
        'auto.offset.reset': 'earliest',
        'group.id' : 'wikimedia-analysis'
    }
    consumer = Consumer(cns_config)
    consumer.subscribe(['eqiad.mediawiki.recentchange'])

    is_bot_count = 0
    is_not_bot_count = 0
    wiki = []
    count = []
    i = 0
    titles = []
    count_title = []
    j = 0

    f = open("data.txt", "a", encoding="utf-8")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Error", msg.error())
                continue
            msg_decoded = msg.value().decode('utf-8')
            event = json.loads(msg_decoded)

            b, nb = bot_tracker(event, cns_config)
            is_bot_count += b
            is_not_bot_count += nb
            event_wiki_count, wiki, count, i = events_wiki(event, cns_config, wiki, count, i)
            event_title_count, titles, count_title, j = events_title(event, cns_config, titles, count_title, j)
            line = f"bot={is_bot_count} | human={is_not_bot_count} | wiki={event_wiki_count} | title={event_title_count}"
            print(line)
            f.write(line + "\n")
    finally:
        f.close()
        consumer.close()

    return is_bot_count, is_not_bot_count


if __name__ == "__main__":
    consumer()