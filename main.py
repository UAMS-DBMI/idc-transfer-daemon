import base64
from google.cloud import storage


def main():
    print("Hello from tcia-to-idc-pipeline!")

    # uploading a very large file
    upload_file("big.dat")

    # a much smaller file
    # upload_file("main.py")


def upload_file(filename):
    client = storage.Client(project='pacific-ethos-162617')
    bucket = client.bucket("posda_submit")

    blob = bucket.blob(filename)

    # blob.upload_from_string("This is a test blob.")
    blob.upload_from_filename(filename)


    blob.reload()
    md5_hex = base64.b64decode(blob.md5_hash).hex()
    print(f"MD5: {md5_hex}")



if __name__ == "__main__":
    main()
