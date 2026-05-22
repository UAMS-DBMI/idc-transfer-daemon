import base64
from google.cloud import storage


def main():
    print("Hello from tcia-to-idc-pipeline!")

    client = storage.Client(project='pacific-ethos-162617')
    bucket = client.bucket("posda_submit")
    blob = bucket.blob("test_blob.txt")

    # blob.upload_from_string("This is a test blob.")
    # blob.upload_from_filename("main.py")

    # uploading a very large file
    blob.upload_from_filename("big.dat")

    blob.reload()
    md5_hex = base64.b64decode(blob.md5_hash).hex()
    print(f"MD5: {md5_hex}")


if __name__ == "__main__":
    main()
