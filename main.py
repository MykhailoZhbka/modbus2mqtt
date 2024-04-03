import json, time, threading
import signal, sys, logging

from pymodbus import (
    # ExceptionResponse,
    # Framer,
    ModbusException,
    pymodbus_apply_logging_config,
)

#from pyModbusTCP.client import ModbusClient
from pymodbus.client import ModbusSerialClient
import paho.mqtt.client as mqtt #import the client1
from multiprocessing import Process, Queue

thread_dict = {}

mqtt_receiver_process = None
mqtt_receiver_thread = None
mqtt_sender_thread = None

modbus_conn = None

connected_flag = False
bad_connection_flag = False
disconnect_flag = False
received_string = ""

terminate_main_thread = threading.Event()

def uintjoiner16bto32bit(high_byte, low_byte): #simple function that joins two 16 bit values to a uint32
	final_value = int("{:016b}".format(int(high_byte)) + "{:016b}".format(int(low_byte)), 2)
	return(final_value)

def intjoiner16bto32bit(high_byte, low_byte): #simple function that joins two 16 bit values to a int32
	high_byte_bits = "{:016b}".format(int(high_byte))
	sign = high_byte_bits[0]
	uint_value = int("{:015b}".format(int(high_byte)) + "{:016b}".format(int(low_byte)), 2)

	if sign == "1":
		final_value = float(0 - uint_value)
	else:
		final_value = uint_value

	return(final_value)

def uinttoint16bit(byte): #This function converts a raw 16bit value from uint to int
	byte_bits = "{:016b}".format(int(byte))
	byte_value = "{:015b}".format(int(byte))
	sign = str(byte_bits[0])
	if sign == "0":
		final_value = int(byte_value, 2)
	else:
		final_value = float(0 - int(byte_value, 2))

	return(final_value)

class MQTTSenderThread:
    def __init__(self, mqtt_client, from_main):
        self.mqtt_client = mqtt_client
        self.from_main = from_main
        self._is_canceled = False
    def start(self):
        self._is_canceled = False
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def _run(self):
        while not self._is_canceled:
            try:
                data = self.from_main.get(False, 1)
                if data:
                    topic = data.get('topic', None)
                    msg = data.get('msg', None)
                    if topic != None and msg != None:
                        jsonstr = json.dumps(msg)
                        self.mqtt_client.publish(topic, json.dumps(msg))
                        logging.info("publish %s:%s", topic, jsonstr)
                time.sleep(0.1)  # Adjust the polling interval as needed
            except Exception as e:
                pass

    def stop(self):
        self._is_canceled = True
        self._thread.join()

class MQTTReceiverThread:
    def __init__(self, to_main, mqtt_settings, modbus_settings, mqtt_sender_queue):
        self.to_main = to_main
        self.mqtt_settings = mqtt_settings
        self.modbus_settings = modbus_settings
        self.mqtt_sender_queue = mqtt_sender_queue
        self._is_canceled = False

    def start(self):
        self._is_canceled = False
        self.mqtt_client = None
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def _run(self):
        while not self._is_canceled:
            try:
                self.mqtt_client = mqtt.Client(None)
                self.mqtt_client.username_pw_set(username=self.mqtt_settings['mqtt_user'], password=self.mqtt_settings['mqtt_pass'])
                self.mqtt_client.connect(self.mqtt_settings['mqtt_server'])
                self.mqtt_client.on_message = mqtt_handler(self.to_main, self.modbus_settings, self.mqtt_sender_queue).process_message
                self.mqtt_client.loop_start()

                self.mqtt_client.subscribe("data/modbus/request")
                
                while not self._is_canceled:
                    time.sleep(0.01)  # Adjust the polling interval as needed
            except Exception as e:
                time.sleep(1)
                pass
        if (self.mqtt_client):
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        
    def stop(self):
        self._is_canceled = True
        self._thread.join()
         
class mqtt_handler: #This is the class which receives the MQTT data
    def __init__(self, to_main, modbus_settings, mqtt_sender_queue):
        self.to_main = to_main
        self.mqtt_sender_queue = mqtt_sender_queue
        self.modbus_settings = modbus_settings
    def process_message(self, client, userdata, message):
        global modbus_conn

        self.to_main.put(message.payload)
        if message.payload:
            #[{'id':0, 'fc':5, 'address':0, 'value':1}]
            msg = message.payload.decode("utf-8").replace("'", '"')
            
            try:
                data = json.loads(msg)
            except json.JSONDecodeError as e:
                logging.info(f"Error decoding JSON: {e}")
                data = None
            
            if data and 'id' in data and 'fc' in data and 'address' in data and 'value' in data:
                data = [data]
                
            for item in data:
                id = int(item['id'])
                fc = int(item['fc'])

                if modbus_conn == None:
                    logging.info ("Modbus connection %d not exist", id)
                    continue

                logging.info(f"writing from modbus fc:{fc}, address:{item['address']}, value:{item['value']}, slave_id:{id}")

                if fc != 5 and fc != 15 and fc != 6:
                    logging.info(f"=== skip fc:%d", fc)
                    continue

                try:
                    err_msg = ''
                    if fc == 5:
                        ret = modbus_conn.write_coil(item['address'], item['value'], slave=id)
                    elif fc == 15:
                        ret = modbus_conn.write_coils(item['address'], item['value'], slave=id)
                    elif fc == 6:
                        ret = modbus_conn.write_register(item['address'], item['value'], slave=id)
                    
                    if ret.isError():
                        if fc == 5:
                            err_msg = f"Could not write coil {item['address']}:{item['value']}, reason:{ret}"
                        elif fc == 15:
                            err_msg = f"Could not write coils {item['address']}:{item['value']}, reason:{ret}"
                        elif fc == 6:
                            err_msg = f"Could not write register {item['address']}:{item['value']}, reason:{ret}"
                        logging.info (f"error_msg: %s", err_msg)
                        msg = {"topic": "system/error/modbus", "msg": err_msg}
                        self.mqtt_sender_queue.put(msg)
                except ModbusException as e:
                    err_msg = f"Failed to write {data['start_addr']}:{data['length']}, reason:{e}"
                    logging.info (f"error_msg: %s", err_msg)
                    msg = {"topic": "system/error/modbus", "msg": err_msg}
                    self.mqtt_sender_queue.put(msg)

class PollingThread:
    def __init__(self, polling_interval, callback):
        self.polling_interval = polling_interval
        self.callback = callback
        self._timer = None
        self._is_canceled = False
        self.callback()
    
    def start(self):
        if not self._is_canceled:
            self._timer = threading.Timer(self.polling_interval, self._run)
            self._timer.start()

    def _run(self):
        if not self._is_canceled:
            self.callback()
            self.start()  # Restart the timer

    def cancel(self):
        self._is_canceled = True
        if self._timer:
            self._timer.cancel()

def process_datapoints(polling_interval, datapoint, local_modbus_conn, poll_timeout, mqtt_sender_queue):
    # Function to be called for each set of datapoints with the same polling interval
    
    # local_modbus_conn = ModbusClient(host=modbus_settings['modbus_server'], port=modbus_settings['port'], 
    #                             unit_id=modbus_settings['unit_id'], auto_open=True, timeout=modbus_settings['timeout'])

    result_holder = {}
    for fc, datas in datapoint.items():
        for data in datas:
            regs = []
            # if (fc == 1): # READ_COILS
            #     regs = local_modbus_conn.read_coils(data["start_addr"], data["length"])
            # elif (fc == 2): # READ_DISCRETE_INPUTS 
            #     regs = local_modbus_conn.read_discreteinputs(data["start_addr"], data["length"])
            # elif (fc == 3): # READ_HOLDING_REGISTERS 
            #     regs = local_modbus_conn.read_holdingregisters(data["start_addr"], data["length"])
            # elif(fc == 4): # READ_INPUT_REGISTERS 
            #     regs = local_modbus_conn.read_inputregisters(data["start_addr"], data["length"])

            logging.info("reading from modbus fc:%d, start:%d, length:%d, slave_id:%d", fc, data["start_addr"], data["length"], data["slave_id"])
            
            if fc < 1 or fc > 6:
                logging.info (f" === skip fc:{fc}")
                continue

            try:
                if (fc == 1 or fc == 5): # READ_COILS
                    regs = local_modbus_conn.read_coils(data["start_addr"], data["length"], slave=data['slave_id'])
                elif (fc == 2): # READ_DISCRETE_INPUTS 
                    regs = local_modbus_conn.read_discrete_inputs(data["start_addr"], data["length"], slave=data['slave_id'])
                elif (fc == 3): # READ_HOLDING_REGISTERS 
                    regs = local_modbus_conn.read_holding_registers(data["start_addr"], data["length"], slave=data['slave_id'])
                elif(fc == 4): # READ_INPUT_REGISTERS 
                    regs = local_modbus_conn.read_input_registers(data["start_addr"], data["length"], slave=data['slave_id'])
            
                if regs.isError():
                    err_msg = f"Modbus library error({regs})"
                    logging.info (f"error_msg: %s", err_msg)
                    msg = {"topic": "system/error/modbus", "msg": err_msg}
                    mqtt_sender_queue.put(msg)
                    continue
                if fc == 1 or fc == 2 or fc == 5:
                    regs = regs.bits
                elif fc == 3 or fc == 4:
                    regs = regs.registers
            except ModbusException as exc:
                err_msg = f"ModbusException({exc}) from library"
                logging.info (f"error_msg: %s", err_msg)
                msg = {"topic": "system/error/modbus", "msg": err_msg}
                mqtt_sender_queue.put(msg)
                local_modbus_conn.close()
                return
            if not regs:
                fail_ts = data.get('fail_ts', None)
                if fail_ts is None:
                    data["fail_ts"] = time.time()
                    fail_ts = data["fail_ts"]
                
                delta = time.time() - fail_ts
                if (delta > poll_timeout):
                    err_msg = f"Timeout to read {data['start_addr']}:{data['length']}"
                    logging.info (f"error_msg: %s", err_msg)
                    msg = {"topic": "system/error/modbus", "msg": err_msg}
                    mqtt_sender_queue.put(msg)
                continue
            
            sub_data = data["sub_data"]
            reg_counter = 0 #Resetting counter for data in each package, separate counter is used as 32bit datas are read from two registers at the same time
            skip_next = False
            for reg in regs: #Looping through the registers in the received package
                curr_reg = data["start_addr"] + reg_counter
                
                if skip_next == False: #If data is 16bit or first package of 32bit
                    info = sub_data[curr_reg]
                    size = info.get('size', 16)
                    signed = info.get('signed', 'False')
                    if int(size) == 32:
                        skip_next = True #Set to true if data is 32bit to skip reading the next package in the loop as it is being read in this run
                        high_byte = regs[reg_counter]
                        low_byte = regs[int(reg_counter) + 1]
                        if signed == "True":
                            final_value = intjoiner16bto32bit(high_byte, low_byte) #Adding high and low byte in 32bit data
                        elif signed == "False":
                            final_value = uintjoiner16bto32bit(high_byte, low_byte) #Adding high and low byte in 32bit data
                    else:
                        if signed == "True":
                            final_value = uinttoint16bit(regs[reg_counter]) #Adding high and low byte in 32bit data
                        elif signed == "False":
                            final_value = regs[reg_counter]

                    if "scaling" in info: #If scaling value is included in setup file
                        final_value = float(final_value) / int(info['scaling']) #Received value is divided with scaling factor
                    #else:
                    #    result_holder[curr_reg] = final_value
                    result_holder[info['name']] = {"friendly_name": info['friendly_name'], "value": final_value, "polling_interval": polling_interval}
                else:
                    skip_next = False #In this instance the current register is being skipped as it is the last part of a 32bit data and has already been processed

                reg_counter = reg_counter + 1 #Adding 1 to the counter    
    
    ## logging.info (f"{result_holder}")
    msg = {"topic": "data/modbus/response", "msg": result_holder}
    mqtt_sender_queue.put(msg)


def on_connect(client, userdata, flags, rc):
    global connected_flag, bad_connection_flag
    if rc == 0:
        connected_flag=True #set flag
        logging.info("MQTT connected OK")
    else:
        bad_connection_flag=True
        logging.info("Bad MQTT connection Returned code=%d",rc)

def on_disconnect(client, userdata, rc):
    global connected_flag, disconnect_flag
    #logging.info("disconnecting reason  "  +str(rc))
    logging.info ("disconnecting reason  "  +str(rc))
    connected_flag=False
    disconnect_flag=True

def on_message(client, userdata, msg):
    global received_string
    if (msg.topic == "config/cabinet"):
        received_string = msg.payload.decode("utf-8")

def terminate_app():
    global thread_dict
    global mqtt_receiver_thread, mqtt_sender_thread
    global terminate_main_thread
    global modbus_conn

    if modbus_conn:
        modbus_conn.close()
    
    if (mqtt_receiver_thread):
        mqtt_receiver_thread.stop()

    if (mqtt_sender_thread):
        mqtt_sender_thread.stop()
   
    for threads in thread_dict.values():
        for t in threads.values():
            t.cancel()  # Cancel existing threads

    thread_dict.clear()  # Clear the dictionary

    terminate_main_thread.set()

    logging.info ("Signal received. Stoppped threads.")    
    sys.exit()

def signal_handler(sig, frame):
    terminate_app()

def main():
    global modbus_conn
    global thread_dict

    global mqtt_receiver_thread
    global mqtt_sender_thread
    global terminate_main_thread

    global received_string
    global connected_flag, disconnect_flag, bad_connection_flag

    old_device_path = ""
    device_path = ""

    mqtt_connecting_timeout = 3
    load_period = 3
    poll_timeout = 30

    bMqttReadSkip = False

    old_config = {}
    config = {}
    mqtt_config = {}
    
    ## logging.basicConfig(filename='modbus2mqtt.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    #pymodbus_apply_logging_config("DEBUG")

    ### read mqtt server information and check
    try:
        with open('default.json', 'r') as file:
            mqtt_config = json.load(file)
    except FileNotFoundError:
        print("Error: default.json not found or could not be opened.")
        sys.exit()
    except json.JSONDecodeError as e:
        logging.info(f"Error decoding JSON: {e}")
        mqtt_config = None

    if not mqtt_config:
        print ("default.json is missing")
        sys.exit()
    
    mqtt_server = mqtt_config.get("mqtt_server")
    if mqtt_server == None:
        print ("mqtt_server field is missing")
        sys.exit()
    
    mqtt_user = mqtt_config.get("mqtt_user")
    if mqtt_user == None:
        print ("mqtt_user field is missing")
        sys.exit()

    mqtt_pass = mqtt_config.get("mqtt_pass")
    if mqtt_pass == None:
        print ("mqtt_pass field is missing")
        sys.exit()
    
    try:
        with open('config.json', 'r') as file:
            old_config = json.load(file)
    except FileNotFoundError:
        print("config.json not found or could not be opened.")
        pass
    except json.JSONDecodeError as e:
        logging.info(f"Error decoding JSON: {e}")
        mqtt_config = None

    signal.signal(signal.SIGINT, signal_handler)

    ### connect to mqtt server
    mqtt_client = mqtt.Client(None)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.loop_start()
    mqtt_client.username_pw_set(username=mqtt_user, password=mqtt_pass)
    
    mqtt_client.connect(mqtt_server)
    
    logging.info(f"mqtt connecting to %s ...", mqtt_server)

    mqtt_conn_try = 0
    while not connected_flag and not bad_connection_flag: #wait in loop
        logging.info("mqtt connecting...")

        mqtt_conn_try = mqtt_conn_try + 1
        if mqtt_conn_try == mqtt_connecting_timeout:
            logging.info("mqtt connecting timeout")
            break

        time.sleep(1)

    ### when mqtt connection timeout or connection is failed
    if mqtt_conn_try == mqtt_connecting_timeout or bad_connection_flag:
        mqtt_client.loop_stop()    #Stop loop
        mqtt_client.disconnect()
        mqtt_client = None
        sys.exit()
    
    mqtt_receiver_queue = Queue()
    mqtt_sender_queue = Queue()

    mqtt_sender_thread = MQTTSenderThread(mqtt_client, mqtt_sender_queue)
    mqtt_sender_thread.start()

    while not terminate_main_thread.is_set():

        ## subscribe "config/cabinet" topic from mqtt server
        logging.info("subscribing topic (config/cabinet)")
        mqtt_client.subscribe("config/cabinet")

        start_load_time = time.time()
        bMqttReadSkip = False

        while not terminate_main_thread.is_set(): 
            delta = round(time.time() - start_load_time, 2)

            if (delta > 3):
                bMqttReadSkip = True
                logging.info("load old cache file")
                break
    
            if len(received_string) > 1:
                try:
                    config = json.loads(received_string.replace("'", '"'))
                except json.JSONDecodeError as e:
                    logging.info(f"Error decoding JSON: {e}")
                    config = None
                received_string = ""
                break

            time.sleep(0.01)
        
        ## when subscribing is failed or config.json is missing
        if not config or bMqttReadSkip:
            if (not old_config):
                logging.info("config information is missing")
                time.sleep(1)
                continue
            config = old_config

        ## when config is not changed
        if old_config == config and modbus_conn:
            time.sleep(load_period)
            continue
        
        old_config = config 
        
        try:
            with open('config.json', 'w') as file:
                file.write(json.dumps(config, indent=4))
        except IOError as e:
            logging.info(f"An error occurred while writing to the file: {e}")

        settings = config.get('plugin', {}).get('modbus')
        if config == None:
            logging.info ("The value ['plugin']['modbus'] is missing")
            continue

        if settings.get('config_update_interval') == None:
            logging.info ("The value ['plugin']['modbus']['config_update_interval'] is missing")
            time.sleep(load_period)
            continue
        
        load_period = int(settings['config_update_interval'])               

        if settings.get('device_update_interval') == None:
            logging.info ("The value ['plugin']['modbus']['device_update_interval'] is missing")
            time.sleep(load_period)
            continue
        
        polling_period = int(settings['device_update_interval']) 

        if settings.get('device_path') == None:
            logging.info ("The value ['plugin']['modbus']['device_path'] is missing")
            time.sleep(load_period)
            continue

        device_path = settings['device_path']

        if settings.get('poll_timeout') == None:
            poll_timeout = 30
        else:
            poll_timeout = int(settings['poll_timeout'])

        devicelists = settings.get('devicelist')
        if devicelists == None:
            logging.info ("The value ['plugin']['modbus']['devicelist'] is missing")
            time.sleep(load_period)
            continue

        if old_device_path != device_path:
            old_device_path = device_path

            if modbus_conn:
                modbus_conn.close()
            
            #modbus_conn = ModbusSerialClient(device_path)
            # serial_port = '/dev/ttyUSB0'
            # baudrate = 9600
            # bytesize = 8
            # parity = 'N'
            # stopbits = 1
            modbus_conn = ModbusSerialClient(method='rtu', port=device_path, baudrate=9600)
            modbus_conn.connect()
            coils = modbus_conn.read_coils(0, 8, slave=1)
            
            if mqtt_receiver_thread:
                mqtt_receiver_thread.stop()

            mqtt_receiver_thread = MQTTReceiverThread(mqtt_receiver_queue, mqtt_config, device_path, mqtt_sender_queue)
            mqtt_receiver_thread.start()
                
        for threads in thread_dict.values():
            for t in threads.values():
                t.cancel()  # Cancel existing threads

        thread_dict.clear()  # Clear the dictionary

        write_info = {}
        read_info = {}
        
        for slave_id, slave_data in devicelists.items():
            unit_id = int(slave_data['id'])

            ## package_maker
            for x, datapoint in slave_data['datapoints'].items():
                datapoint['name'] = x
                fc = datapoint['fc']
                addr = int(datapoint['address'])
                polling_interval = int(datapoint.get('polling_interval', polling_period))
                size = 2 if datapoint.get('size', 1) == 32 else datapoint.get('size', 1)

                if fc >= 1 and fc <= 5:
                    bFind = False

                    if (read_info.get(polling_interval) == None):
                        read_info[polling_interval] = {}
                    
                    if read_info[polling_interval].get(fc) == None:
                        read_info[polling_interval][fc] = []

                    for info in read_info[polling_interval][fc]:
                        if (addr > 0 and addr == info['start_addr'] - 1):
                            bFind = True
                            info['start_addr'] = info['start_addr'] + size
                            break
                        elif (addr == info['end_addr']):
                            bFind = True
                            info['end_addr'] += size
                            break

                    if not bFind:
                        obj = {"slave_id":unit_id, "start_addr": addr, "end_addr": addr + size, "length": size, "sub_data": { addr: datapoint }}
                        read_info[polling_interval][fc].append(obj)
                    else:
                        info['length'] += size
                        info['sub_data'][addr] = datapoint
                else:
                    bFind = False
                    if (write_info.get(fc) == None):
                        write_info[fc] = []

                    for info in write_info[fc]:
                        if (addr > 0 and addr == info['start_addr'] - 1):
                            bFind = True
                            info.start_addr = info['start_addr'] + size
                            break
                        elif (addr == info['end_addr']):
                            bFind = True
                            info['end_addr'] += size
                            break

                    if not bFind:
                        obj = {"slave_id":unit_id, "start_addr": addr, "end_addr": addr + size, "length": size, "sub_data": { addr: datapoint }}
                        write_info[fc].append(obj)
                    else:
                        info['length'] += size
                        info['sub_data'][addr] = datapoint
                #logging.info(f"=== {x}: {datapoint}")
            
            #logging.info(f"\nwrite_info:\n", write_info)
            #logging.info(f"\nread_info:\n", read_info, "\n")

            # register_loop
            thread_dict[unit_id] = {}
            for period, datas in read_info.items():
                thread_dict[unit_id][period] = PollingThread(period, lambda p=period, d=datas: 
                                                             process_datapoints(p, d, modbus_conn, poll_timeout, mqtt_sender_queue))
                thread_dict[unit_id][period].start()

        time.sleep(load_period)

if __name__ == "__main__":
    main()
