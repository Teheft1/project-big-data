import os

from selenium import webdriver
import requests
import io, time
from PIL import Image
from confluent_kafka import SerializingProducer
from datetime import datetime


def get_image(download_path, url, file_name):
    try:
        image_content = requests.get(url).content
        image_file = io.BytesIO(image_content)
        image = Image.open(image_file)
        byte_img = io.BytesIO()
        # file_path = download_path + file_name + ".jpg"
        #
        # with open(file_path, "wb") as f:
        #     image.tobytes()
        # # f, "JPEG"
        image.save(byte_img, format='JPEG')
        return byte_img.getvalue()
    except Exception as e:
        print(e)


def report_producer(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic} ")
        # [{msg.partition()}]


def main():
    cService = webdriver.ChromeService()
    driver = webdriver.Chrome(service=cService)

    image_url = (
        "https://jid.jasamarga.com/cctv2/1815a00?tx=1646733773121&t=20220915110931778"
    )
    topic = "Image_streaming"
    producer = SerializingProducer({"bootstrap.servers": "localhost:9092"})

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 120:
        try:
            img_produce = get_image("",image_url,"image1.jpg")
            print(img_produce)
            producer.produce(topic, value=img_produce, on_delivery=report_producer)

            time.sleep(5)
        except Exception as e:
            print(e)
        finally:
            producer.flush()


if __name__ == "__main__":
    main()