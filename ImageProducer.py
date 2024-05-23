import os

from selenium import webdriver
import requests
import io, time
from PIL import Image
from confluent_kafka import serializing_producer
from datetime import datetime


def get_image(download_path, url, file_name):
    try:
        image_content = requests.get(url).content
        image_file = io.BytesIO(image_content)
        image = Image.open(image_file)
        file_path = download_path + file_name + ".jpg"

        with open(file_path, "wb") as f:
            image.tobytes()
        # f, "JPEG"
        print("Image saved successfully")
        return file_path
    except Exception as e:
        print(e)


def report_producer(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")


def main():
    cService = webdriver.ChromeService()
    driver = webdriver.Chrome(service=cService)

    image_url = (
        "https://jid.jasamarga.com/cctv2/1815a00?tx=1646733773121&t=20220915110931778"
    )
    topic = "Image_streaming"
    producer = serializing_producer({"bootstrap-server": "localhost:9091"})

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 120:
        try:
            img_produce = get_image("",image_url,"image1.jpg")
        except Exception as e:
            print(e)
