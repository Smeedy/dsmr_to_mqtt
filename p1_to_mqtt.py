#!/usr/bin/env python3

from dsmr_parser import telegram_specifications
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V2_2
from dsmr_parser import obis_references

import json
import paho.mqtt.client as mqtt
import paho.mqtt.publish as mqtt_publish

import time

MQTT_HOSTNAME='localhost'
MQTT_PORT=1883
MQTT_TOPIC='dsmr/up'
MQTT_CLIENT_ID='dsmr1'

serial_reader = SerialReader(
    device='/dev/ttyUSB0',
    serial_settings=SERIAL_SETTINGS_V2_2,
    telegram_specification=telegram_specifications.V2_2
)

def publish(payload):

    return mqtt_publish.single(
        MQTT_TOPIC,
        payload=payload,
        qos=0, retain=False,
        hostname=MQTT_HOSTNAME, port=MQTT_PORT, client_id=MQTT_CLIENT_ID,
        keepalive=60, will=None, protocol=mqtt.MQTTv311, transport="tcp"
    )


prev_u_t1 = None
prev_u_t2 = None
prev_d_t1 = None
prev_d_t2 = None
prev_gas = None

prev_epoch = -1
prev_epoch_gas = int(time.time()) # need for delta every hour

for telegram in serial_reader.read():
    #print(telegram)


    u_t1= float(telegram[obis_references.ELECTRICITY_USED_TARIFF_1].value)
    u_t2 = float(telegram[obis_references.ELECTRICITY_USED_TARIFF_2].value)
    d_t1 = float(telegram[obis_references.ELECTRICITY_DELIVERED_TARIFF_1].value)
    d_t2 = float(telegram[obis_references.ELECTRICITY_DELIVERED_TARIFF_2].value)
    t_active =  int(telegram[obis_references.ELECTRICITY_ACTIVE_TARIFF].value)
    gas = float(telegram[obis_references.GAS_METER_READING].value)
    
    epoch = int(time.time())
                
    
    d = {
      'elec': {
          'equip_id': telegram[obis_references.EQUIPMENT_IDENTIFIER].value,
          'counter': {
              't_active': t_active,
              'used_t1_in_kwh': u_t1,
              'used_t2_in_kwh': u_t2,
              'delivered_t1_in_kwh': d_t1,
              'delivered_t2_in_kwh': d_t2,
          }
      },
      'gas': {
          'equip_id': telegram[obis_references.EQUIPMENT_IDENTIFIER_GAS].value,
          'counter': {
              'used_in_m3': gas
          }
      }
    }

    if prev_epoch > 0:

        # ELEC
        delta_s = epoch - prev_epoch
        
        d['elec']['delta'] = {
            'period_in_s': delta_s,
            'used_t1_in_kwh': round(u_t1 - prev_u_t1, 3),
            'used_t2_in_kwh': round(u_t2 - prev_u_t2, 3),
            'delivered_t1_in_kwh': round(d_t1 - prev_d_t1, 3),
            'delivered_t2_in_kwh': round(d_t2 - prev_d_t2, 3)
        }

        # GAS
        delta_s_gas = epoch - prev_epoch_gas
        delta_gas_m3 = round(gas - prev_gas, 3) if prev_gas is not None else 0
        if delta_s_gas >= 3600 or delta_gas_m3 > 0:
            # gas only capable of updating every hour in P1...
            d['gas']['delta'] = {
                'period_in_s': delta_s_gas,
                'used_in_m3': delta_gas_m3
            }

            prev_gas = gas
            prev_epoch_gas = epoch
            

            
    prev_epoch = epoch
    prev_u_t1 = u_t1
    prev_u_t2 = u_t2
    prev_d_t1 = d_t1
    prev_d_t2 = d_t2

              
    print(json.dumps(d))
    publish(json.dumps(d))
    
    
