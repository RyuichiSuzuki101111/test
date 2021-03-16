import hmac
import hashlib
import json
from enum import Enum, auto
import datetime
from urllib import request
import mysql.connector
from functools import reduce
import time

class Market(Enum):
    BTC_JPY = auto()
    ETH_JPY = auto()
    FX_BTC_JPY = auto()
    ETH_BTC = auto()
    BCH_BTC = auto()
    def tablename(self):
        return "EXECUTIONS_{}".format(self.name)

def query(**kwargs):
    return '?' + '&'.join(map(lambda key:'{}={}'.format(key, kwargs[key]), kwargs.keys()))

class Bitflyer:

    def __init__(self, market: Market):
        with open('key', 'r', encoding='shiftjis') as key:
            self.key = key.readline().replace('\n','')
            self.secret = key.readline().replace('\n','')
            self.market = market
            self.mydb = mysql.connector.connect(
                host="localhost",
                user="ryuichi",
                password="krakowiak",
                database="bitflyer"
            )

    def board(self):
        path  = '/v1/board'
        return self.__public_api_request(path, query(product_code=self.market.name))

    def ticker(self):
        path = '/v1/ticker'
        return self.__public_api_request(path, query(product_code=self.market.name))

    def executions(self, **kwargs):
        path  = '/v1/executions'
        kwargs['product_code'] = self.market.name
        return self.__public_api_request(path, query(**kwargs))

    def boardstate(self):
        path  = '/v1/getboardstate'
        return self.__public_api_request(path, query(product_code=self.market.name))

    def health(self):
        path  = '/v1/gethealth'
        return self.__public_api_request(path, query(product_code=self.market.name))

    def chats(self):
        path  = '/v1/getchats'
        return self.__public_api_request(path, query(product_code=self.market.name))

    def permissions(self):
        method = 'GET'
        path   = '/v1/me/getpermissions'
        return self.__private_api_request(method, path)

    def balance(self):
        method = 'GET'
        path   = '/v1/me/getbalance'
        return self.__private_api_request(method, path)

    def collateral(self):
        method = 'GET'
        path   = '/v1/me/getcollateral'
        return self.__private_api_request(method, path)

    def collateralaccounts(self):
        method = 'GET'
        path   = '/v1/me/getcollateralaccounts'
        return self.__private_api_request(method, path)

    def addresses(self):
        method = 'GET'
        path   = '/v1/me/getaddresses'
        return self.__private_api_request(method, path)

    def coinins(self, query):
        method = 'GET'
        path   = '/v1/me/getcoinins'
        return self.__private_api_request(method, path, query=query)

    def coinouts(self, query):
        method = 'GET'
        path   = '/v1/me/getcoinouts'
        return self.__private_api_request(method, path, query=query)

    def bankaccounts(self, query):
        method = 'GET'
        path   = '/v1/me/getbankaccounts'
        return self.__private_api_request(method, path)

    def sendchildorder(self, child_order_type, side, price, size, minute_to_expire, time_in_force):
        method = 'POST'
        path   = '/v1/me/sendchildorder'
        body   = {
            'product_code': self.market.name,
            'child_order_type': child_order_type,
            'side': side,
            'price': price,
            'size': size,
            'minute_to_expire': minute_to_expire,
            'time_in_force': time_in_force
        }
        return self.__private_api_request(method, path, body=body)

    def __public_api_request(self, path, query=''):
        url = 'https://api.bitflyer.com' + path + query
        return request.Request(url, method='GET')

    def __private_api_request(self, method, path, query='', body={}):

        if len(body) > 0:
            jstr = json.dumps(body)
        else:
            jstr = ''

        timestamp = str(datetime.datetime.now())
        msg = (timestamp + method + path + jstr).encode()
        sign = hmac.new(self.secret.encode(), msg, hashlib.sha256).hexdigest()

        __headers = {
            'ACCESS-KEY': self.key,
            'ACCESS-TIMESTAMP': timestamp,
            'ACCESS-SIGN': sign,
            'Content-Type': 'application/json'
        }

        url = 'https://api.bitflyer.com' + path

        if jstr == '':
            return request.Request(url, headers=__headers, method=method)
        else:
            return request.Request(url, headers=__headers, method=method, data=jstr.encode())

    def fetch_executions(self):
        cursor = self.mydb.cursor()
        query = "INSERT IGNORE INTO {} (id, side, price, size, timestamp, origin) VALUES ".format(self.market.tablename())

        id = 2136128361
        flag = True
        while flag:
            req = self.executions(before=id, count=500)
            with request.urlopen(req) as res:
                message = json.loads(res.read().decode("utf-8"))
                print(message)
                id = message[len(message)-1]["id"]
                cursor.execute("SELECT * from {} where id = {}".format(self.market.tablename(), id))
                n = cursor.fetchall()
                print(n)
                query_ = query + ','.join(map(lambda m: "({}, {}, {}, {}, {}, {})".format(m["id"], '"{}"'.format(m["side"]), m["price"], m["size"],'"{}"'.format(m["exec_date"].replace("T", " ").replace("Z", "")), 1), message)) + ";"
                cursor.execute(query_)
                self.mydb.commit()
            time.sleep(0.7)
            

if __name__ == "__main__":

    def delete_duplication():
        db = mysql.connector.connect(
                host="localhost",
                user="ryuichi",
                password="krakowiak",
                database="bitflyer"
        )
        

    bf = Bitflyer(Market.BTC_JPY)
    bf.fetch_executions()