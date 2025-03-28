import requests
proxy = {
    "http": "http://70qd7kEak:RNW78Fm5@gate.lilproxy.site:3000",
    "https": "http://70qd7kEak:RNW78Fm5@gate.lilproxy.site:3000"
}
print(requests.get("https://api.ipify.org?format=json", proxies=proxy, timeout=10).json())