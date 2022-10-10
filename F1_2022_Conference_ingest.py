###########################################################################################
####                 Custom Code to run F1 Ingest as a Forwarding Script
import time
import signalfx
import configparser
import argparse
import json
import requests
import background
import urllib3
from datetime import datetime
from f1_22_telemetry.listener import TelemetryListener
from requests.adapters import HTTPAdapter, Retry

urllib3.disable_warnings()
background.n = 40

global hostname
global player_name
global SIM
global mode
global sesh  # HTTP Session for Splunk HEC with Keep Alive
global motion
global telemetry
global lap
global status
global player_info  # Details of current player
global lap_info  # track events such as lap and sector completion for filtering

parser = argparse.ArgumentParser(description="Splunk DataDrivers")
parser.add_argument("--hostname", help="Hostname", default="host_1")
parser.add_argument("--player", help="Player Name", default="Drivey McDriverface")
parser.add_argument("--port", help="UDP Port", type=int, default=20777)
parser.add_argument("--o11y", help="Send data to O11y Cloud", choices=["yes", "no"], default="no")
parser.add_argument("--splunk", help="Send data to Splunk Enterprise/Cloud", choices=["yes", "no"], default="yes")
# mode should be "Spectator" to grab all cars, "Solo" to only grab data for the player car
parser.add_argument("--mode", help="Spectator or Solo Mode", choices=["spectator", "solo"], default="spectator")
args = vars(parser.parse_args())

hostname = args["hostname"]
player_name = args["player"]
mode = args["mode"]

sesh = requests.Session()
retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
sesh.mount("https://", HTTPAdapter(pool_connections=80, pool_maxsize=80, max_retries=0, pool_block=False))

# Open config file for read
config = configparser.ConfigParser()
config.read("settings.ini")
# Set Debugging
debug = config.getboolean("ingest_settings", "debug")
# Splunk Enterprise Variables
splunk_hec_ip = config.get("ingest_settings", "splunk_hec_ip")
splunk_hec_port = config.get("ingest_settings", "splunk_hec_port")
splunk_hec_token = config.get("ingest_settings", "splunk_hec_token")
# SIM variables
sim_token = config.get("ingest_settings", "sim_token")
sim_endpoint = config.get("ingest_settings", "ingest_endpoint")
# Telemetry varilables     
motion = config.getboolean("telemetry_settings", "motion")
telemetry = config.getboolean("telemetry_settings", "telemetry")
lap = config.getboolean("telemetry_settings","lap")
status = config.getboolean("telemetry_settings","status")

client = signalfx.SignalFx(ingest_endpoint=sim_endpoint)
ingest = client.ingest(sim_token)

print("Hostname: " + args["hostname"])
print("Player Name: " + args["player"])
print("UDP Port: " + str(args["port"]))
print("Splunk O11y Cloud Data: " + args["o11y"])
print("Splunk Enterprise/Cloud Data: " +args["splunk"])
print("Solo or Spectator: " + args["mode"])
print("Debug: " + str(debug))
print("Splunk HEC Endpoint: " + splunk_hec_ip)
print("Splunk O11y Cloud Ingest Endpoint: " + sim_endpoint)
print("Car telemetry enabled: " + str(telemetry))
print("Car motion enabled: " + str(motion))
print("Car lap enabled: " + str(lap))
print("Car status enabled: " + str(status))

#########################################
# Set up global variables and data stores

player_dict = {
    "ai_controlled": 1,
    "driver_id": 63,
    "name": "",
    "nationality": 13,
    "race_number": 6,
    "team_id": 3,
    "your_telemetry": 1
}

player_info = [player_dict]

lap_info = []

for i in range(1, 21):
    lap_info = lap_info + [
        {
            "current_sector": 0,
            "current_lap": 1,
            "lap_event": "none",
            "lap_event_count": 0
        }
    ]

#########################################
# Set up lists of SIM Metrics and Dimensions
# This is taylored for F1 2022 Telemetry format
sim_metrics = [
    "air_temperature",
    "brake",
    "brakes_temperature1",
    "brakes_temperature2",
    "brakes_temperature3",
    "brakes_temperature4",
    "car_position",
    "current_lap_num",
    "current_lap_time_in_ms",
    "engine_rpm",
    "engine_temperature",
    "g_force_lateral",
    "g_force_longitudinal",
    "g_force_vertical",
    "gear",
    "speed",
    "sector",
    "throttle",
    "track_temperature",
    "tyres_inner_temperature1",
    "tyres_inner_temperature2",
    "tyres_inner_temperature3",
    "tyres_inner_temperature4",
    "tyres_surface_temperature1",
    "tyres_surface_temperature2",
    "tyres_surface_temperature3",
    "tyres_surface_temperature4"
]

sim_dimensions = [
    "name",
    "player_name"
]


def lookup_packet_id(packet_id):
    dict = {
        0: "MotionData",
        1: "SessionData",
        2: "LapData",
        3: "EventData",
        4: "ParticipantsData",
        5: "CarSetupData",
        6: "CarTelemetryData",
        7: "CarStatusData",
        8: "FinalClassificationData",
        9: "LobbyInfoData",
        10: "CarDamageData",
        11: "SessionHistoryData",
        99: "ScriptStartup",
    }
    return dict[packet_id]


# Sends metrics to SIM
@background.task
def send_metric(f1_metrics, f1_dimensions):
    telemetry_json = []
    f1_dimensions['f1-2022-hostname'] = hostname

    for key, value in f1_metrics.items():
        telemetry_json.append({"metric": "f1_2022." + key, "value": value, "dimensions": f1_dimensions})

    ingest.send(gauges=telemetry_json)


def send_dims_and_metrics(f1_json):
    dimensions = [{key: car_dict[key] for key in sim_dimensions if key in car_dict} for car_dict in f1_json]

    metrics = [{key: car_dict[key] for key in sim_metrics if key in car_dict} for car_dict in f1_json]
    
    for f1_metrics, f1_dimensions in zip(metrics, dimensions):
        if len(f1_metrics) >= 1:
            # Send current row to SIM
            send_metric(f1_metrics, f1_dimensions)


# Function to send raw unprocessed event to hec
@background.task
def send_hec_json(data, packet_id):
    global hostname
    global player_name
    global sesh

    hec_payload = ""

    event = {}
    event["time"] = datetime.now().timestamp()
    event["sourcetype"] = lookup_packet_id(packet_id)
    event["source"] = "f1_2022"
    event["host"] = hostname
    event["event"] = data

    hec_payload = hec_payload + json.dumps(event)
    
    url = str(splunk_hec_ip + ":" + splunk_hec_port + "/services/collector")
    header = {"Authorization": "{}".format("Splunk " + splunk_hec_token)}

    try:
        response = sesh.post(url=url, data=hec_payload, headers=header, verify=False)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(err)
        print("Packet ID affected: " + lookup_packet_id(packet_id))
    

# function to send multiple events to splunk enterprise env
@background.task
def send_hec_batch(event_rows, packet_id):
    event_rows = [{key: str(dict[key]) for key in dict.keys()} for dict in event_rows]

    hec_payload = ""

    for row in event_rows:
        event = {}
        # event["time"] = float(row["checkpoint_1_data_received"])
        event["time"] = datetime.now().timestamp()
        event["sourcetype"] = lookup_packet_id(packet_id)
        event["source"] = "f1_2022"
        event["host"] = hostname
        event["event"] = row

        hec_payload = hec_payload + json.dumps(event)

    url = str(splunk_hec_ip + ":" + splunk_hec_port + "/services/collector")
    header = {"Authorization": "{}".format("Splunk " + splunk_hec_token)}

    try:
        response = sesh.post(url=url, data=hec_payload, headers=header, verify=False)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(err)
        print("Packet ID affected: " + lookup_packet_id(packet_id))


#########################################
# Data Stream Management and Processing
def update_player_info(data):
    global player_info
    player_info = data["participants"]


# Flatten the F1-2022 sections that consist of fields, and lists of tyre info
def flatten_fast(data):
    blank_list = []
    car_index = 0
    for entry in data:
        blank_dict = {}
        blank_dict.update({element: entry[element] for element in entry if not isinstance(entry[element], list)})
        multi_value_fields = {element: {element + str(ind + 1): value for ind, value in enumerate(entry[element])} for element in entry if isinstance(entry[element], list)}

        for field in multi_value_fields:
            blank_dict.update(multi_value_fields[field])

        blank_dict.update({"car_index": car_index})
        car_index += 1
        blank_list = blank_list + [blank_dict]

    telemetry = blank_list

    return telemetry


def flatten_car(data):
    data = flatten_fast(data)

    if debug == True:
        for entry in data:
            entry.update({"checkpoint_2_payload_flattened": time.time()})

    return data


def merge_car_telemetry(data, header, playerCarIndex):
    # Flatten file format
    telemetry = flatten_car(data["car_telemetry_data"])

    # Augment with header and player info data
    for entry, player in zip(telemetry, player_info):
        entry.update(player)
        entry.update(header)

    # If not in spectator mode, get rid of the non-player cars
    if mode != "spectator":
        # get only player car from flattened data
        telemetry = [telemetry[playerCarIndex]]
        telemetry[0].update({"player_name": player_name})

    return telemetry


def merge_car_motion(data, header, playerCarIndex):
    # Flatten file format
    telemetry = flatten_car(data["car_motion_data"])

    # Augment with header and player info data
    for entry, player in zip(telemetry, player_info):
        entry.update(player)
        entry.update(header)

    # If not in spectator mode, get rid of the non-player cars
    if mode != "spectator":
        # get only player car from flattened data
        telemetry = [telemetry[playerCarIndex]]
        telemetry[0].update({"player_name": player_name})
        # Get additional motion data, not stored in the main array
        """
        telemetry[0].update({"angular_acceleration_x": data["angular_acceleration_x"]})
        telemetry[0].update({"angular_acceleration_y": data["angular_acceleration_y"]})
        telemetry[0].update({"angular_acceleration_z": data["angular_acceleration_z"]})
        telemetry[0].update({"angular_velocity_x": data["angular_velocity_x"]})
        telemetry[0].update({"angular_velocity_y": data["angular_velocity_y"]})
        telemetry[0].update({"angular_velocity_z": data["angular_velocity_z"]})

        telemetry[0].update({"frontWheelsAngle": data["frontWheelsAngle"]})
        telemetry[0].update({"local_velocity_x": data["local_velocity_x"]})
        telemetry[0].update({"local_velocity_y": data["local_velocity_y"]})
        telemetry[0].update({"local_velocity_z": data["local_velocity_z"]})
        """
        # Get the per-wheel motion data for the player car
        player_car_motion_list = [
            "suspension_acceleration",
            "suspension_position",
            "suspension_velocity",
            "wheel_slip",
            "wheel_speed",
        ]
        for motion_data in player_car_motion_list:
            i = 1
            for value in data[motion_data]:
                telemetry[0].update({motion_data + str(i): value})
                i += 1

    return telemetry


def merge_car_status(data, header, playerCarIndex):
    # Flatten file format
    telemetry = flatten_car(data["car_status_data"])

    # Augment with header and player info data
    for entry, player in zip(telemetry, player_info):
        entry.update(player)
        entry.update(header)

    # If not in spectator mode, get rid of the non-player cars
    if mode != "spectator":
        # get only player car from flattened data
        telemetry = [telemetry[playerCarIndex]]
        telemetry[0].update({"player_name": player_name})

    return telemetry


def merge_car_lap(data, header, playerCarIndex):
    # Flatten file format
    telemetry = flatten_car(data["lap_data"])

    # Augment with header and player info data
    for entry, player in zip(telemetry, player_info):
        entry.update(player)
        entry.update(header)

    # check for events such as lap or sector completeion
    for entry, info_buffer in zip(telemetry, lap_info):
        if info_buffer["current_lap"] < entry["current_lap_num"]:
            info_buffer.update({"lap_event": "LAP_COMPLETE", "lap_event_count": 0})
            entry.update({"lap_event": "LAP_COMPLETE"})

        if info_buffer["current_sector"] < entry["sector"]:
            info_buffer.update({"lap_event": "SECTOR_COMPLETE", "lap_event_count": 0})
            entry.update({"lap_event": "SECTOR_COMPLETE"})

        # repeat event anouncement for 3 seconds in case of network loss
        if info_buffer["lap_event"] != "none":
            if info_buffer["lap_event_count"] < 5:
                entry.update(
                    {
                        "lap_event": info_buffer["lap_event"],
                        "lap_event_count": info_buffer["lap_event_count"],
                    }
                )

                info_buffer.update({"lap_event_count": info_buffer["lap_event_count"] + 1})
            else:
                entry.update({"lap_event": "none"})
                info_buffer.update({"lap_event": "none"})
                info_buffer.update({"lap_event_count": 0})

        info_buffer.update({"current_sector": entry["sector"], "current_lap": entry["current_lap_num"]})
        entry.update({"lap_event": info_buffer["lap_event"]})

    # lap_info +[{'current_sector': 0,
    #            'current_lap': 1,
    #            'lap_event': 'none',
    #            'lap_event_count': 0}]

    # If not in spectator mode, get rid of the non-player cars
    if mode != "spectator":
        # get only player car from flattened data
        telemetry = [telemetry[playerCarIndex]]
        telemetry[0].update({"player_name": player_name})

    return telemetry


def merge_car_setups(data, header, playerCarIndex):
    # Flatten file format
    telemetry = flatten_car(data["car_setups"])

    # Augment with header and player info data
    for entry, player in zip(telemetry, player_info):
        entry.update(player)
        entry.update(header)

    # If not in spectator mode, get rid of the non-player cars
    if mode != "spectator":
        # get only player car from flattened data
        telemetry = [telemetry[playerCarIndex]]
        telemetry[0].update({"player_name": player_name})

    return telemetry


def merge_session(data, header, playerCarIndex):
    # Flatten file format
    telemetry = flatten_car(data["marshal_zones"])

    # Augment with header and player info data
    for entry, player in zip(telemetry, player_info):
        entry.update(player)
        entry.update(header)
        # Grab root data elements

        entry.update({"air_temperature": data["air_temperature"]})
        entry.update({"track_id": data["track_id"]})
        entry.update({"weather": data["weather"]})
        entry.update({"total_laps": data["total_laps"]})
        entry.update({"track_temperature": data["track_temperature"]})
        entry.update({"track_length": data["track_length"]})
        #entry.update({"spectator_car_index": data["spectator_car_index"]})
        """
        entry.update({"gamePaused": data["gamePaused"]})
        entry.update({"isSpectating": data["isSpectating"]})
        entry.update({"m_formula": data["m_formula"]})
        entry.update({"networkGame": data["networkGame"]})
        entry.update({"numMarshalZones": data["numMarshalZones"]})
        entry.update({"numWeatherForecastSamples": data["numWeatherForecastSamples"]})
        entry.update({"pitSpeedLimit": data["pitSpeedLimit"]})
        entry.update({"safetyCarStatus": data["safetyCarStatus"]})
        entry.update({"sessionDuration": data["sessionDuration"]})
        entry.update({"sessionTimeLeft": data["sessionTimeLeft"]})
        entry.update({"sessionType": data["sessionType"]})
        entry.update({"sliProNativeSupport": data["sliProNativeSupport"]})
        """

    # If not in spectator mode, get rid of the non-player cars
    if mode != "spectator":
        # get only player car from flattened data
        telemetry = [telemetry[playerCarIndex]]
        telemetry[0].update({"player_name": player_name})

    return telemetry


def merge_final(data, header, playerCarIndex):
    # Flatten file format
    telemetry = flatten_car(data["classification_data"])

    # Augment with header and player info data
    for entry, player in zip(telemetry, player_info):
        entry.update(player)
        entry.update(header)
        # Grab root data elements
        entry.update({"num_cars": data["num_cars"]})

    # If not in spectator mode, get rid of the non-player cars
    if mode != "spectator":
        # get only player car from flattened data
        telemetry = telemetry.iloc[[playerCarIndex]]
        telemetry["player_name"] = player_name

    return telemetry


def merge_lobby(data, header, playerCarIndex):
    # Flatten file format
    telemetry = flatten_car(data["lobby_players"])

    # Augment with header and player info data
    for entry, player in zip(telemetry, player_info):
        entry.update(player)
        entry.update(header)
        # Grab root data elements
        entry.update({"num_players": data["num_players"]})

    # If not in spectator mode, get rid of the non-player cars
    if mode != "spectator":
        # get only player car from flattened data
        telemetry = telemetry.iloc[[playerCarIndex]]
        telemetry["player_name"] = player_name

    return telemetry


def send_augmented_json(data, packet_id):
    data.update({"player_name": player_name})
    send_hec_json(data, packet_id)


@background.task
def massage_data(data):
    dict_object = data.to_json()
    data = json.loads(dict_object)
    packet_id = data["header"]["packet_id"]
    header = data["header"]
    playerCarIndex = data["header"]["player_car_index"]

    if debug == True:
        data["header"].update({"checkpoint_1_data_received": time.time()})

    if packet_id == 0:
        if motion == True:
            merged_data = merge_car_motion(data, header, playerCarIndex)
        else:
            return

    if packet_id == 1:
        merged_data = merge_session(data, header, playerCarIndex)

    if packet_id == 2:
        if lap == True:
            merged_data = merge_car_lap(data, header, playerCarIndex)
        else:
            return

    if packet_id == 3:
        try:
            if args["splunk"] == "yes":
                send_augmented_json(data, packet_id)
        except Exception as e:
            print(str(e))
        return

    if packet_id == 4:
        update_player_info(data)

    if packet_id == 5:
        merged_data = merge_car_setups(data, header, playerCarIndex)

    if packet_id == 6:
        if telemetry == True:
            merged_data = merge_car_telemetry(data, header, playerCarIndex)
        else:
            return

    if packet_id == 7:
        if status == True:
            merged_data = merge_car_status(data, header, playerCarIndex)
        else:
            return

    if packet_id == 8:
        merged_data = merge_final(data, header, playerCarIndex)

    if packet_id == 9:
        merged_data = merge_lobby(data, header, playerCarIndex)

    merged_data = [entry for entry in merged_data if entry['name']!=""]
    # merged_data = merged_data[merged_data['name']!=""]

    if debug == True:
        for entry in merged_data:
            entry.update({"checkpoint_3_payload_processed": time.time()})

    # send data to HEC
    if args["splunk"] == "yes":
        send_hec_batch(merged_data, packet_id)

    # send data to SIM
    if args["o11y"] == "yes":
        send_dims_and_metrics(merged_data)




# Initialise session
startup_payload = {
    "message": "Script Starting",
    "description": "Initialising Script",
    "checkpoint_1_data_received": datetime.now().timestamp()
    }

if args["splunk"] == "yes":
    send_hec_batch([startup_payload], 99)

listener = TelemetryListener(port=args["port"])
while True:
    packet = listener.get()
    massage_data(packet)
