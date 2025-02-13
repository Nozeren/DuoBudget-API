import requests

result = requests.post('http://127.0.0.1:8000/users', json={"name": "Mario", "color": "red"})
print(result)