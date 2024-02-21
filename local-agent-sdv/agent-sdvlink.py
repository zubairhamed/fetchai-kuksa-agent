from uagents import Agent, Context, Model
from uagents.setup import fund_agent_if_low
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

DATATYPE_MAPPING = {
    "uns": DataType.UNSPECIFIED,
    "str": DataType.STRING,
    "bool": DataType.BOOLEAN, 
    "int8": DataType.INT8,
    "int16": DataType.INT16,
    "int32": DataType.INT32,
    "int64": DataType.INT64,
    "unt8": DataType.UINT8,
    "unt16": DataType.UINT16,
    "unt32": DataType.UINT32,
    "unit64": DataType.UINT64,
    "float": DataType.FLOAT,
    "double": DataType.DOUBLE,
    "ts": DataType.TIMESTAMP
}

class SdvLinkAgentRegistrationRequest(Model):
    driverId: str = Field(description="Unique Driver ID identifying an agent")
    agent_address: str = Field(description="Agent Address")

class SdvLinkRemoteSetRequest(Model):
    path: str = Field(description="Direct Path to Covesa VSS Value")
    value: str =  Field(description="Value for Covesa VSS Property")    
    valueType: str =  Field(description="VSS Value Type")    
    driverId: str = Field(description="Driver/Vehicle ID for which the message is intended")

class SdvLinkRemoteGetRequest(Model):
    path: str = Field(description="Direct Path to Covesa VSS Value")
    driverId: str = Field(description="Driver/Vehicle ID for which the message is intended")    

class SdvLinkResponse(Model):
    success: bool

DRIVER_ID = "ABC123"
API_KEY = "f00bca33-afca-4e93-bb71-d55a5c1fe37e"
AGENT_NAME="sdvlink"
SEED_PHRASE="secret seed phrase"
MAILBOX_STR=f"{API_KEY}@https://agentverse.ai/"
AGENTVERSE_AGENT_ADDRESS = "agent1q0k8w9k0dszq49s73ljww3u7mmf2xkpfsszdumaymugp9zm7nvme7jf2sf8"
DATABROKER_ADDRESS = "localhost"
DATABROKER_PORT = 55555

sdvlink = Agent(
    name=AGENT_NAME,
    seed=SEED_PHRASE,
    mailbox=MAILBOX_STR
)
 
fund_agent_if_low(sdvlink.wallet.address())
vssClient = VSSClient(DATABROKER_ADDRESS, DATABROKER_PORT) 

@sdvlink.on_event("startup")
async def startup(ctx: Context):
    try:        
        async with vssClient:
            entries = await vssClient.get(entries=[ 
                EntryRequest("Vehicle.VersionVSS.Major", View.ALL, (VssField.UNSPECIFIED,)),
                EntryRequest("Vehicle.VersionVSS.Minor", View.ALL, (VssField.UNSPECIFIED,)),
                EntryRequest("Vehicle.VersionVSS.Patch", View.ALL, (VssField.UNSPECIFIED,))
            ])

            ctx.logger.info(f"Connected to Kuksa Databroker ver. major {entries[0].value.value}, minor {entries[1].value.value}, patch {entries[2].value.value}")

            updates = (EntryUpdate(DataEntry(
                'Vehicle.Driver.Identifier.Subject',
                value=Datapoint(value=DRIVER_ID),
                metadata=Metadata(data_type=DataType.STRING),
            ), (VssField.VALUE,)),)
            await vssClient.set(updates=updates)
    except Exception as err:
        ctx.logger.error(f"Unable to connect to Kuksa Databroker {err}. Connection Details: {DATABROKER_ADDRESS} port {DATABROKER_PORT}")
        exit(1)

    ctx.logger.info(f"Registering my address {sdvlink.address}")
    await ctx.send(AGENTVERSE_AGENT_ADDRESS, SdvLinkAgentRegistrationRequest(driverId=DRIVER_ID, agent_address=sdvlink.address))

@sdvlink.on_message(model=SdvLinkRemoteGetRequest)
async def handle_vss_get_request(ctx: Context, sender: str, msg: SdvLinkRemoteGetRequest):
    path = msg.path
    driverId = msg.driverId

    # TODO: How to get value and send to chat interface?
    
    await ctx.send(sender, SdvLinkResponse(success=True))

@sdvlink.on_message(model=SdvLinkRemoteSetRequest, replies=SdvLinkResponse)
async def handle_vss_set_request(ctx: Context, sender: str, msg: SdvLinkRemoteSetRequest):  
    path = msg.path
    value = msg.value
    driverId = msg.driverId
    valueType = msg.valueType

    ctx.logger.info(f"Got request from deltav {msg}")

    if (driverId != DRIVER_ID):
        ctx.logger.info("Message was not intended for this driver ID. Mismatched Driver ID")
        return
    
    # Construct and Send Message to Databroker
    try:        
        async with vssClient:
            updates = (EntryUpdate(DataEntry(
                path,
                value=Datapoint(value=value),
                metadata=Metadata(data_type=DATATYPE_MAPPING[valueType]),
            ), (VssField.VALUE,)),)
            await vssClient.set(updates=updates)
    except Exception as err:
        ctx.logger.error(f"Unable to retrieve value from Kuksa Databroker {err}. Connection Details: {DATABROKER_ADDRESS} port {DATABROKER_PORT}")
        await ctx.send(sender, SdvLinkResponse(success=False))
        return

    await ctx.send(sender, SdvLinkResponse(success=True))

if __name__ == "__main__":
    sdvlink.run()
