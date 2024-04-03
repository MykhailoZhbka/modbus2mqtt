#! /bin/sh

pip install -r requirements.txt
pyinstaller --onefile --windowed main.py
cp ./dist/main /usr/local/bin/modbus2mqtt
cp ./modbus-config-cache.json /usr/local/bin
cp ./modbus2mqtt.service /usr/lib/systemd/system/
systemctl daemon-reload
systemctl restart modbus2mqtt
