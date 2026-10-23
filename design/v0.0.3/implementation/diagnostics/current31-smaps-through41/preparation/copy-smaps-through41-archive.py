"""Install this independent evidence folder only after its read-only verification. Never modify through39 payload."""
import argparse
from pathlib import Path
import shutil
import subprocess
import sys

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--stage',type=Path,default=Path(__file__).resolve().parents[1])
parser.add_argument('--destination',type=Path,required=True,help='NEW independent diagnostics subdirectory; must not exist')
args=parser.parse_args()
if args.destination.exists():
    parser.error('destination exists; refusing to overwrite any original or prior archive')
if args.stage.resolve()==args.destination.resolve() or args.stage.resolve() in args.destination.resolve().parents:
    parser.error('destination must be outside the stage')
subprocess.run([sys.executable,'-B',str(args.stage/'preparation/verify-smaps-through41-archive.py'),'--base',str(args.stage)],check=True)
shutil.copytree(args.stage,args.destination)
subprocess.run([sys.executable,'-B',str(args.destination/'preparation/verify-smaps-through41-archive.py'),'--base',str(args.destination)],check=True)
print('Installed independent archive:',args.destination)
