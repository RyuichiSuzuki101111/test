import asyncio
import websockets
import json
from functools import reduce
import mysql.connector

def message(method, params):
  return json.dumps({
    "jsonrpc": "2.0",
    "method": method,
    "params": params
  })

async def main():

  mydb = mysql.connector.connect(
    host="localhost",
    user="ryuichi",
    password="krakowiak",
    database="bitflyer"
  )
  cursor = mydb.cursor()

  def insert(message):
    for m in message:
      id = m["id"]
      side = '"{}"'.format(m["side"])
      price = m["price"]
      size = m["size"]
      exec_date = '"{}"'.format(m["exec_date"].replace("T", " ").replace("Z", ""))
      print("INSERT INTO executions_btc_jpy(id, side, price, size, timestamp) values({},{},{},{},{})".format(id, side, price, size, exec_date))
      cursor.execute("INSERT INTO executions_btc_jpy(id, side, price, size, timestamp) values({},{},{},{},{})".format(id, side, price, size, exec_date))
      mydb.commit()

  async with websockets.connect("wss://ws.lightstream.bitflyer.com/json-rpc") as ws:
      await ws.send(message("subscribe", {"channel": "lightning_executions_BTC_JPY"}))
      while True:
          data = await ws.recv()
          obj = json.loads(data)
          m = obj['params']['message']
          insert(m)

if __name__ == "__main__":
  asyncio.get_event_loop().run_until_complete(main())