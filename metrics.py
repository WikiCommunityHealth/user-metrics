import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import json

with open('./community_ca.json') as input:
    andamanto = json.loads(input.read())
mesi = list(sorted(andamanto['per_mese'].keys()))
y = list(map(lambda k: andamanto['per_mese'][k], mesi))
x = list(map(lambda k: datetime.strptime(k, "%Y_%m").date(), mesi))

fig, ax = plt.subplots()
ax.plot(x, y)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax.set(xlabel='mese', ylabel='gente attiva', title='Grafico')
ax.grid()

fig.savefig("test.png")
plt.show()
