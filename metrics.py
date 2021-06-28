import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import json

with open('./community_ca.json') as input:
    andamanto = json.loads(input.read())

with open('./community_ca_maschio.json') as input:
    andamanto_maschio = json.loads(input.read())

with open('./community_ca_femmina.json') as input:
    andamanto_femmina = json.loads(input.read())

with open('./community_ca_boh.json') as input:
    andamanto_boh = json.loads(input.read())


mesi = list(sorted(andamanto['per_mese'].keys()))
y = list(map(lambda k: andamanto['per_mese'][k], mesi))
x = list(map(lambda k: datetime.strptime(k, "%Y_%m").date(), mesi))
plt.plot(x, y, 'k', label = "total", )

mesi = list(sorted(andamanto_maschio['per_mese'].keys()))
y = list(map(lambda k: andamanto_maschio['per_mese'][k], mesi))
x = list(map(lambda k: datetime.strptime(k, "%Y_%m").date(), mesi))
plt.plot(x, y, 'b', label = "maschio", )

mesi = list(sorted(andamanto_femmina['per_mese'].keys()))
y = list(map(lambda k: andamanto_femmina['per_mese'][k], mesi))
x = list(map(lambda k: datetime.strptime(k, "%Y_%m").date(), mesi))
plt.plot(x, y, 'r', label = "femmina", )


mesi = list(sorted(andamanto_boh['per_mese'].keys()))
y = list(map(lambda k: andamanto_boh['per_mese'][k], mesi))
x = list(map(lambda k: datetime.strptime(k, "%Y_%m").date(), mesi))
plt.plot(x, y, 'g', label = "boh", )

plt.show()
