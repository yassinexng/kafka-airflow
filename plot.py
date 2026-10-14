import matplotlib.pyplot as plt

bots = []
humans = []

f = open("data.txt", "r", encoding="utf-8")

for line in f:
    parts = line.split(" | ")
    bot_part = parts[0]
    human_part = parts[1]

    bot_number = bot_part.split("=")[1]
    human_number = human_part.split("=")[1]

    bots.append(int(bot_number))
    humans.append(int(human_number))

f.close()

plt.figure(figsize=(10, 5))
plt.plot(bots, label="bots")
plt.plot(humans, label="humans")
plt.xlabel("event number")
plt.ylabel("cumulative count")
plt.title("Wikimedia recent changes: bots vs humans")
plt.legend()
plt.grid(True)
plt.savefig("bots_vs_humans.png", dpi=120)