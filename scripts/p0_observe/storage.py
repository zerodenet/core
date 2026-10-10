"""Private bounded files; never truncate a prior acceptance run."""
import json
import os


class LimitReached(Exception):
    pass


def encode(record):
    return (json.dumps(record, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n").encode()


def private_file(path):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    return os.fdopen(os.open(path, flags, 0o600), "wb")


class Journal:
    def __init__(self, directory, max_bytes):
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.path = directory / "samples.jsonl"
        self.file = private_file(self.path)
        self.max_bytes, self.bytes_written = max_bytes, 0

    def write(self, record):
        data = encode(record)
        # Reserve a small end record even when samples hit the cap.
        if self.bytes_written + len(data) > self.max_bytes - 4096:
            raise LimitReached()
        self.file.write(data)
        self.file.flush()
        self.bytes_written += len(data)

    def finish(self, record):
        data = encode(record)
        if self.bytes_written + len(data) > self.max_bytes:
            raise LimitReached()
        self.file.write(data)
        self.file.close()


def save_json(path, value):
    with private_file(path) as output:
        output.write(encode(value))
