"""Read ELF geometry only, never execute the input binary."""
import hashlib
import os
from pathlib import Path
import struct


def identity(binary):
    path = Path(binary)
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        stream.seek(0)
        header = stream.read(64)
        if header[:6] != b'\x7fELF\x02\x01':
            raise ValueError('diagnostic is fixed to original x86-64 little-endian ELF64')
        fields = struct.unpack('<16sHHIQQQIHHHHHH', header)
        phoff, phentsize, phnum = fields[5], fields[9], fields[10]
        if phentsize != 56 or phnum > 1024:
            raise ValueError('unexpected ELF program header geometry')
        segments = []
        for number in range(phnum):
            stream.seek(phoff + number * phentsize)
            kind, flags, offset, vaddr, _, filesz, memsz, align = struct.unpack('<IIQQQQQQ', stream.read(56))
            if kind in (1, 0x6474e552):
                segments.append(dict(kind='LOAD' if kind == 1 else 'GNU_RELRO', flags=flags,
                    offset=offset, vaddr=vaddr, filesz=filesz, memsz=memsz, align=align))
    stat = path.stat()
    return dict(path=str(path), sha256=digest, inode=stat.st_ino, device_major=os.major(stat.st_dev),
                device_minor=os.minor(stat.st_dev), bytes=stat.st_size, elf_type=fields[1],
                page_bytes=os.sysconf('SC_PAGE_SIZE'), segments=segments)
