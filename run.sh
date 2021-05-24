#!/bin/sh
export FLASK_APP=ir.py
while true
do
  flask run --host=127.0.0.1 --port=5002
done