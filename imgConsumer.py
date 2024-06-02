from confluent_kafka import Consumer, KafkaError
from minio import Minio
from minio.error import S3Error
from PIL import Image
import os
import io


# def save_image(image_bytes, file_path):
#     try:
#         os.makedirs(os.path.dirname(file_path), exist_ok=True)
#         image = Image.open(io.BytesIO(image_bytes))
#         image.save(file_path, format='JPEG')
#         print(f"Image saved to {file_path}")
#     except Exception as e:
#         print(f"Failed to save image: {e}")


def main():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'minio-group',
        'auto.offset.reset': 'earliest'
    })

    minio_client = Minio(
        "localhost:9000",
        access_key="consumer",
        secret_key="ANO7nbvRRyIrrcMhtonkRvIZlB41tGVxVOCMNl0n",
        secure=False
    )

    bucket_name = "kafka-minio-bucket"

    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    consumer.subscribe(["Image_streaming"])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            image_bytes = msg.value()
            timestamp = msg.timestamp()[1]
            # file_path = f"./received_image_{timestamp}.jpg"
            # save_image(image_bytes, file_path)

            minio_object_name = f"{timestamp}.jpg"
            minio_client.put_object(bucket_name, minio_object_name, io.BytesIO(image_bytes), len(image_bytes), 'image/jpeg')
            print(f"Uploaded image to MinIO: {minio_object_name}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()