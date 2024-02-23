'''
MIT License

Copyright (c) 2024 Mohammad Zubair, Abdulrahman AlKoptan

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

from pydantic import Field
import asyncio
from kuksa_client.grpc.aio import VSSClient
from kuksa_client.grpc import Datapoint
from kuksa_client.grpc import DataEntry
from kuksa_client.grpc import DataType
from kuksa_client.grpc import EntryUpdate
from kuksa_client.grpc import Field as VssField
from kuksa_client.grpc import Metadata
from kuksa_client.grpc import EntryRequest
from kuksa_client.grpc import View
from kuksa_client.grpc import SubscribeEntry
from colorama import init
from colorama import Fore
import calendar
import time
from datetime import datetime

from web3 import Web3, AsyncWeb3

init(autoreset=True)

DATABROKER_ADDRESS = "40.114.167.144"
DATABROKER_PORT = 55555
PAYMENT_PATH = "Vehicle.VehicleIdentification.VehicleSpecialUsage"   # VSS Path co-opted for payment
BLOCKCHAIN_ADDRESS = "ws://158.177.1.17:8546"


vssClient = VSSClient(DATABROKER_ADDRESS, DATABROKER_PORT) 

"""
What code does:
- Vehicle.VehicleIdentification.OptionalExtras will be used for both incoming and outgoing messsages
- Subscribes to Vehicle.VehicleIdentification.OptionalExtras
- Format of value is a string with pipe delimited values
- handlePayment will be called 


Data is expected to come in from Vehicle.VehicleIdentification.VehicleSpecialUsage 
Format:
Inbound >>>|TS|ID<str>|Tx-Type<str>|Value<str>
for example:
>>>|2024-02-21-09:23:45|DriverABC|Fuel|223.56 (Fuel is the goods, 223.56 is the payment amount)
>>>|2024-02-21-09:23:45|DriverABC|Parking|60  (Parking is the goods, 60 is the parking duration in minutes)

Outbound <<<|TS|transactionid|message
example: <<<|2024-02-21-09:23:45|TX123456|Transaction Complete. Payment has been made
"""

def logError(msg):
    log(f"{Fore.RED} {msg}")

def log(msg):
    current_GMT = time.gmtime()
    ts = calendar.timegm(current_GMT)
    dt = datetime.fromtimestamp(ts)
    print(f"{Fore.GREEN} {dt} {msg}")

async def subscribe():
    """ Subscribe to values used by app and sync changes """
    print(f"   {Fore.YELLOW}>>> {Fore.RED}Subscribing required values..{Fore.YELLOW}<<<")
    print("")

    async with vssClient:
        entries = []
        entries.append (SubscribeEntry(PAYMENT_PATH, View.FIELDS, (VssField.VALUE, VssField.ACTUATOR_TARGET)))
 
        async for updates in vssClient.subscribe(entries=entries): 
            for update in updates:
                print("Incoming message: " + update.entry.value.value.values[0])
                if update.entry.value is not None:
                    frags = parseInbound(update.entry.value.value.values[0])
                    
                    if isinstance(frags, str):
                        print(f"Unexpected or invalid content returned. Ignoring content: {frags}")
                        continue                        
                    
                    ts = frags[1]
                    identity = frags[2]
                    goods = frags[3]
                    paymentAmount = frags[4]
                    await handlePayment(ts, identity, goods, paymentAmount)
 
def parseInbound(val):
    frags = val.split("|")

    if frags[0] == "<<<":
        return "Ignoring outbound message"

    if len(frags) != 6:
        return "Unexpected parameters encountered. Expected 6"

    if frags[0] != ">>>":
        return "Unexpected marker encountered. Expecting '>>>'"

    try:
        float(frags[4])
    except ValueError:
        return "Unable to parse Price. Price is not float data."

    return frags

async def handlePayment(ts, identity, goods, paymentAmount):
    print(f"handlePayment {ts} {identity} {goods} {paymentAmount}")
    """ Handle SmartContract Payments and then write to databroker the results """
    
    w3 = Web3(Web3.WebsocketProvider(BLOCKCHAIN_ADDRESS))
    print(f"Connected to blockchain: {w3.is_connected()}")

    returnValue = f"<<<|<TimeStamp>|<ReturnMessage>"                    
    try:
        async with vssClient:
            entry = EntryUpdate(DataEntry(PAYMENT_PATH, value=Datapoint(value=returnValue), metadata=Metadata(data_type=DataType.STRING_ARRAY)), (VssField.VALUE,))
            await vssClient.set(updates=(entry,))
    except Exception as err:
        logError(f"ERROR: Sending data to Kuksa Databroker {err}. Connection Details: {DATABROKER_ADDRESS} port {DATABROKER_PORT}")
    pass

asyncio.run(subscribe())

