"""Sparse read-only proc snapshots tied to a real process generation."""
import hashlib
import json
import os
from pathlib import Path
import time
from elf_metadata import identity
from smaps_parse import parse, summarize

LIMIT = 16*1024*1024


def start_ticks(pid):
    value=(Path('/proc')/str(pid)/'stat').read_text()
    return int(value[value.rindex(')')+2:].split()[19])


class MappingProbe:
    def __init__(self,binary,output):
        # Potentially expensive hash/ELF reads happen BEFORE product Popen.
        self.binary=identity(binary)
        self.output=output/'memory'
        self.output.mkdir()
        (self.output/'binary-elf-identity.json').write_text(json.dumps(self.binary,indent=2)+'\n')
        self.events=[]
        self.errors=[]
        self.process=None
        self.generation=None

    def attach(self,process,popen_started_ns,popen_returned_ns):
        self.process=process
        self.popen_started_ns=popen_started_ns
        self.popen_returned_ns=popen_returned_ns
        self.capture('popen_return',full=False)

    def capture(self,label,full=True):
        requested=time.monotonic_ns()
        number=len(self.events)
        directory=self.output/f'{number:02}-{label}'
        directory.mkdir()
        row=dict(label=label,full=full,requested_ns=requested,capture_started_ns=time.monotonic_ns(),
            pid=self.process.pid,popen_started_ns=self.popen_started_ns,popen_returned_ns=self.popen_returned_ns,
            sequential_nonatomic_reads=True,files={},errors=[])
        root=Path('/proc')/str(self.process.pid)
        try:
            generation=start_ticks(self.process.pid)
            if self.generation is None:self.generation=generation
            if generation!=self.generation:raise ProcessLookupError('PID generation changed')
            row['process_start_ticks']=generation
            stat=(root/'exe').stat()
            row['exe']={'link':os.readlink(root/'exe'),'inode':stat.st_ino,
                'device_major':os.major(stat.st_dev),'device_minor':os.minor(stat.st_dev),
                'matches_frozen_binary_identity':(stat.st_ino,os.major(stat.st_dev),os.minor(stat.st_dev))==
                    (self.binary['inode'],self.binary['device_major'],self.binary['device_minor']),
                'frozen_sha256':self.binary['sha256']}
            if not row['exe']['matches_frozen_binary_identity']:
                raise ProcessLookupError('actual exe differs from prehashed frozen inode')
            files=('status','smaps_rollup','maps','smaps') if full else ('status','smaps_rollup')
            contents={}
            for name in files:
                started=time.monotonic_ns()
                try:
                    with (root/name).open('rb') as stream:content=stream.read(LIMIT+1)
                    if len(content)>LIMIT:
                        raise ValueError(f'{name} exceeds explicit diagnostic read cap; snapshot incomplete')
                    (directory/name).write_bytes(content)
                    contents[name]=content
                    row['files'][name]={'bytes':len(content),'sha256':hashlib.sha256(content).hexdigest(),
                        'read_started_ns':started,'read_finished_ns':time.monotonic_ns()}
                except OSError as error:
                    row['errors'].append({'file':name,'error':repr(error)})
            if start_ticks(self.process.pid)!=generation:
                raise ProcessLookupError('PID changed during sequential read')
            if full and 'smaps' in contents:
                rows=parse(contents['smaps'].decode())
                account=summarize(rows,self.binary)
                account['maps_and_smaps_same_headers']=(
                    [(x['start'],x['end'],x['permissions'],x['offset'],x['inode']) for x in parse(contents.get('maps',b'').decode())] ==
                    [(x['start'],x['end'],x['permissions'],x['offset'],x['inode']) for x in rows])
                summary=json.dumps(account,indent=2).encode()+b'\n'
                (directory/'mapping-account.json').write_bytes(summary)
                row['mapping_account_sha256']=hashlib.sha256(summary).hexdigest()
                row['mapping_count']=len(rows)
                row['maps_and_smaps_same_headers']=account['maps_and_smaps_same_headers']
        except (OSError,ValueError,ProcessLookupError) as error:
            row['errors'].append({'point':label,'error':repr(error)})
            row['process_exit_observed']=self.process.poll()
        row['capture_finished_ns']=time.monotonic_ns()
        row['capture_elapsed_ns']=row['capture_finished_ns']-row['capture_started_ns']
        row['snapshot_complete']=not row['errors']
        # An exited address space is unavailable, never a fabricated zero-RSS snapshot.
        row['expected_post_exit_unavailable']=bool(label in ('shutdown_after_stdin_close', 'process_exit_or_unavailable') and row['errors'] and self.process.returncode is not None)
        if row['errors'] and not row['expected_post_exit_unavailable']:self.errors.append(row)
        self.events.append(row)
        (directory/'capture.json').write_text(json.dumps(row,indent=2)+'\n')
        (self.output/'index.json').write_text(json.dumps({'scope':'diagnostic only; sparse synchronous added observer pauses',
            'events':self.events,'errors':self.errors},indent=2)+'\n')
        return row
