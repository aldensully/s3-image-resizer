import os
import boto3
from PIL import Image
from io import BytesIO
from dotenv import load_dotenv
import os

load_dotenv()

#OG Bucket to read from
FROM_BUCKET = ""

#Optional nested folder to read from
FROM_FOLDER_PREFIX = ""

#Bucket to place into
TO_BUCKET = "" 

#Optional nested folder to put into 
TO_FOLDER_PREFIX = "" 


SIZE = 500 

s3 = boto3.client('s3')

def download_image(key):
    response = s3.get_object(Bucket=FROM_BUCKET, Key=key)
    return response['Body'].read()

def upload_image(key, image_data):
    s3.put_object(Bucket=TO_BUCKET, Key=key, Body=image_data)

def resize_image(image_data):
    with Image.open(BytesIO(image_data)) as img:
        img.thumbnail((SIZE,SIZE))
        img_byte_arr = BytesIO()
        img.save(img_byte_arr, format='JPEG')
        return img_byte_arr.getvalue()

def process_images():
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=FROM_BUCKET,Prefix=FROM_FOLDER_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.lower().endswith(('.png', '.jpg', '.jpeg')):

                #do image processing
                print(f"Processing {key}...")
                image_data = download_image(FROM_BUCKET, key)
                resized_image_data = resize_image(image_data)
                
                #remove folder prefix from image name 
                textWithoutFolder = key.replace(FROM_FOLDER_PREFIX, "", 1)
                new_key = TO_FOLDER_PREFIX + textWithoutFolder
                upload_image(new_key, resized_image_data)
                print(f"Uploaded resized image to {new_key}")

if __name__ == "__main__":
    process_images()

