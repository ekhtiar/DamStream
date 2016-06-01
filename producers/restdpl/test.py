import requests

r = requests.get('http://api.fixer.io/latest')

print r.content