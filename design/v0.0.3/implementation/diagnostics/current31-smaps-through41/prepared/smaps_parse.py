"""VMA-granularity resident accounting: never apportion partial VMA RSS by size."""
from collections import defaultdict
import os
from pathlib import Path
import re

HEADER = re.compile(r'^([0-9a-f]+)-([0-9a-f]+)\s+(\S+)\s+([0-9a-f]+)\s+([0-9a-f]+):([0-9a-f]+)\s+(\d+)\s*(.*)$')
METRICS = ('Size','Rss','Pss','Shared_Clean','Shared_Dirty','Private_Clean','Private_Dirty','Anonymous','Swap','SwapPss','Locked')


def parse(text):
    mappings = []
    for line in text.splitlines():
        match = HEADER.match(line)
        if match:
            start, end, perms, offset, major, minor, inode, path = match.groups()
            row = dict(start=int(start,16), end=int(end,16), permissions=perms,
                offset=int(offset,16), device_major=int(major,16), device_minor=int(minor,16),
                inode=int(inode), path=path, metrics_bytes={})
            mappings.append(row)
        elif mappings:
            name, _, value = line.partition(':')
            if name in METRICS:
                words = value.split()
                if len(words) != 2 or words[1] != 'kB':
                    raise ValueError('unexpected smaps metric unit')
                mappings[-1]['metrics_bytes'][name] = int(words[0])*1024
    return mappings


def same_file(row, binary):
    return all(row[key] == binary[key] for key in ('inode','device_major','device_minor'))


def load_biases(rows, binary):
    page = binary['page_bytes']
    down = lambda value: value//page*page
    votes = defaultdict(int)
    for row in rows:
        if not same_file(row,binary):
            continue
        for segment in binary['segments']:
            if segment['kind']=='LOAD' and row['offset']==down(segment['offset']):
                votes[row['start']-down(segment['vaddr'])] += 1
    if not votes:
        return []
    maximum = max(votes.values())
    return sorted(bias for bias,count in votes.items() if count == maximum)


def other_file_category(row, cache):
    key = (row['device_major'],row['device_minor'],row['inode'],row['path'])
    if key not in cache:
        category = 'other_file'
        try:
            path = Path(row['path'].removesuffix(' (deleted)'))
            stat = path.stat()
            if (os.major(stat.st_dev),os.minor(stat.st_dev),stat.st_ino) == key[:3]:
                with path.open('rb') as stream:
                    header = stream.read(18)
                if header[:6] == b'\x7fELF\x02\x01' and int.from_bytes(header[16:18],'little') == 3:
                    category = 'other_mapped_dynamic_elf'
        except (OSError,ValueError):
            category = 'unverified_other_file'
        cache[key] = category
    return cache[key]


def summarize(rows, binary):
    biases = load_biases(rows,binary)
    page = binary['page_bytes']
    relro = []
    if len(biases)==1:
        for segment in binary['segments']:
            if segment['kind']=='GNU_RELRO':
                # Loader protections are page granularity; record interval rather
                # than assuming section SHF_WRITE is the final VMA protection.
                low=(biases[0]+segment['vaddr'])//page*page
                high=(biases[0]+segment['vaddr']+segment['memsz'])//page*page
                relro.append((low,high))
    totals, cache = {}, {}
    for row in rows:
        path, perms = row['path'], row['permissions']
        if same_file(row,binary):
            overlap=sum(max(0,min(row['end'],end)-max(row['start'],start)) for start,end in relro)
            row['gnu_relro_overlap_bytes'] = overlap if len(biases)==1 else None
            if 'x' in perms:
                category='binary_RX' if 'w' not in perms else 'binary_WX'
            elif 'w' in perms:
                category='binary_RW'
            elif len(biases)!=1:
                category='binary_R_RELRO_unresolved'
            elif overlap==row['end']-row['start'] and overlap:
                category='binary_R_RELRO'
            elif overlap:
                category='binary_R_mixed_RELRO_not_apportioned'
            else:
                category='binary_R_other'
        elif path=='[heap]': category='brk_heap_label'
        elif path=='[stack]' or path.startswith('[stack:'): category='stack_label'
        elif row['inode'] and path.startswith('/'): category=other_file_category(row,cache)
        elif path.startswith('[v'): category='kernel_special_mapping'
        elif not row['inode']: category='other_anonymous_not_assumed_heap'
        else: category='other_mapping'
        row['category']=category
        bucket=totals.setdefault(category,{name:0 for name in METRICS})
        for name,value in row['metrics_bytes'].items(): bucket[name]+=value
    return dict(categories_bytes=totals, mappings=rows, candidate_load_biases=biases,
        gnu_relro_protected_intervals=relro,
        limitations=['Metrics are per VMA, not allocations. RssAnon is not heap.',
            'Mixed RELRO VMA RSS is not divided proportionally.',
            'Anonymous zero-fill of ELF LOAD remains anonymous unless separately proven.',
            'other_mapped_dynamic_elf verifies device/inode/ELF ET_DYN, not logical library ownership.'])
