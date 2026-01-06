import requests

url = "https://exports.conviu.com/open/u70usd1xo1y5fknhd1xm8kixkdsya8po/writer/zr18y3chmd810xjm2sphwqhskbnyzpbf.xml"

r = requests.get(url, timeout=30)

print("STATUS:", r.status_code)
print("CONTENT-TYPE:", r.headers.get("Content-Type"))
print("\n--- POCZĄTEK ODP ---")
print(r.text[:1000])
