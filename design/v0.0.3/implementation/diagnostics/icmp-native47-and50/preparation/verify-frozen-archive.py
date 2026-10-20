import argparse,gzip,hashlib,json
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--base',type=Path,required=True);p.add_argument('--originals',action='store_true');a=p.parse_args()
sha=lambda b:hashlib.sha256(b).hexdigest()
d=json.loads((a.base/'manifest.json').read_text());assert d['overall_passed'] is False
for r in d['records']:
 z=(a.base/r['path']).read_bytes();b=gzip.decompress(z)
 assert (len(z),sha(z),len(b),sha(b))==(r['gzip_bytes'],r['gzip_sha256'],r['raw_bytes'],r['raw_sha256']),r['path']
 if a.originals:assert Path(r['source']).read_bytes()==b,r['source']
print(json.dumps({'records_verified':len(d['records']),'rounds':len(d['rounds']),'originals_checked':a.originals,'all17_passed':False}))
