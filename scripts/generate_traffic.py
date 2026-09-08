import argparse, json, time, random, urllib.request
p=argparse.ArgumentParser();p.add_argument("--seconds",type=int,default=60);p.add_argument("--url",default="http://localhost:7000/checkout");a=p.parse_args()
end=time.time()+a.seconds
while time.time()<end:
    data=json.dumps({"amount":round(random.uniform(10,500),2)}).encode()
    req=urllib.request.Request(a.url,data=data,headers={"Content-Type":"application/json"})
    try: urllib.request.urlopen(req,timeout=3)
    except Exception: pass
    time.sleep(0.05)
print("traffic generation complete")
