import time
import socket
from minio import Minio
from minio.error import S3Error


# ==== FUNCTIONS ====
def wait_for_service(host: str, port: int, timeout=30, max_retries=3, retry_interval=5):
    attempt = 0
    while attempt < max_retries:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                print(f"Service available at {host}:{port}")
                return
        except OSError:
            attempt += 1
            print(f"Waiting for service at {host}:{port}... Retry {attempt}/{max_retries}")
            time.sleep(retry_interval)

    raise TimeoutError(f"Service at {host}:{port} not available after {max_retries} attempts")
        

def get_minio_client(host: str, port: str, username: str, password: str) -> Minio:
    return Minio(
        f"{host}:{port}",
        access_key=username,
        secret_key=password,
        secure=False
    )


def create_buckets(client: Minio, buckets: list[str]):
    for bucket in buckets:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"Bucket '{bucket}' created successfully.")
            else:
                print(f"Bucket '{bucket}' already exists.")
        except S3Error as e:
            print(f"Error creating bucket '{bucket}': {e}")


# ==== MAIN ====
def main():
    MINIO_HOST = "minio"
    MINIO_PORT = 9000
    ACCESS_KEY = "minio"
    SECRET_KEY = "minio123"
    BUCKETS = ["lakehouse"]

    try:
        wait_for_service(MINIO_HOST, MINIO_PORT)
        client = get_minio_client(host=MINIO_HOST, port=MINIO_PORT, username=ACCESS_KEY, password=SECRET_KEY)
        create_buckets(client, BUCKETS)
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
