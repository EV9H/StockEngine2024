#!/bin/bash
python -m amazon_kclpy.kcl --properties consumer.properties --executor "python ./kinesis_consumer.py"