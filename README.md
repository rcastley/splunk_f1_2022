# splunk_f1_2022
Ingest script for O11y and Core/Enterprise

```
python3 -m venv venv
. /venv/bin/activate
pip3 install -r requirements.txt
```

```
usage: F1_2022_Conference_ingest.py [-h] [--hostname HOSTNAME]
                                    [--player PLAYER] [--port PORT]
                                    [--o11y {yes,no}] [--splunk {yes,no}]
                                    [--mode {spectator,solo}]

Splunk DataDrivers

options:
  -h, --help            show this help message and exit
  --hostname HOSTNAME   Hostname
  --player PLAYER       Player Name
  --port PORT           UDP Port
  --o11y {yes,no}       Send data to O11y Cloud
  --splunk {yes,no}     Send data to Splunk Enterprise/Cloud
  --mode {spectator,solo}
                        Spectator or Solo Mode
```
