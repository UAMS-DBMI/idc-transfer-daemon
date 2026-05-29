import base64
from pathlib import Path

from google.cloud import storage

SERVICE_ACCOUNT_JSON = "sa-key.json"
PROJECT = "tcia-data-transfers"
BUCKET_NAME = "posda_submit"


def main() -> None:
    bucket = get_bucket()
    upload_directory(bucket, Path("random_data"))


def get_bucket() -> storage.Bucket:
    client = storage.Client.from_service_account_json(
        SERVICE_ACCOUNT_JSON, project=PROJECT
    )
    return client.bucket(BUCKET_NAME)


def upload_directory(bucket: storage.Bucket, directory: Path) -> None:
    if not directory.is_dir():
        raise NotADirectoryError(f"Not a directory: {directory}")

    for path in sorted(directory.iterdir()):
        if path.is_file():
            upload_file(bucket, path)


def upload_file(bucket: storage.Bucket, path: Path) -> None:
    object_name = path.as_posix()
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(path))

    blob.reload()
    md5_hex = base64.b64decode(blob.md5_hash).hex()
    print(f"Uploaded {object_name} (MD5: {md5_hex})")


if __name__ == "__main__":
    main()
