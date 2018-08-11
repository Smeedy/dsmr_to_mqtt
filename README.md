# dsmr_to_mqtt
Draft for pushing DSMR json messages on an MQTT topic.

This snippet will read the serial port and will parse the DSMR message. It will create a JSON structure and will 
post the counters on an MQTT topic. It will also calculate deltas from the 2nd message and up and will annotate the message.

WIP
