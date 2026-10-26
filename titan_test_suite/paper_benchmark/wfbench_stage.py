from __future__ import annotations
import ast,base64,json,os,shlex,subprocess,sys,time

def decode(text):
    return json.loads(base64.urlsafe_b64decode(text+"="*(-len(text)%4)).decode())

payload=decode(sys.argv[1]); command=payload["command"]
args=[]
for item in command.get("arguments",[]):
    text=str(item)
    if text.startswith("--input-files "):
        parsed=ast.literal_eval(text.split(" ",1)[1])
        for name in parsed:
            path=os.path.abspath(name); os.makedirs(os.path.dirname(path),exist_ok=True)
            open(path,"ab").close()
        text="--input-files "+json.dumps(parsed,separators=(",",":"))
    elif text.startswith("--output-files "):
        parsed=ast.literal_eval(text.split(" ",1)[1])
        text="--output-files "+json.dumps(parsed,separators=(",",":"))
    if text.startswith("--") and " " in text:
        flag,value=text.split(" ",1); args.extend([flag,value])
    else:
        args.extend(shlex.split(text))
binary=os.environ.get("WFBENCH_BIN",os.path.expanduser("~/titan-paper-venv/bin/wfbench"))
t0=time.time(); cp=subprocess.run([binary,*args],capture_output=True,text=True,timeout=180)
print(json.dumps({"task":payload["name"],"returncode":cp.returncode,"seconds":time.time()-t0,
                  "stdout":cp.stdout[-1000:],"stderr":cp.stderr[-1000:]}))
raise SystemExit(cp.returncode)
