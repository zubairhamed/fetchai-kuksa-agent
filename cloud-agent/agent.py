from ai_engine import UAgentResponse, UAgentResponseType, KeyValue
from uagents import Context, Model, Protocol
from pydantic import Field

### Write code for the new module here and import it from agent.py.
class UnsupportedVssPropertyError(Exception):
    pass

class InvalidVssValueError(Exception):
    pass

class VssMeta:
    def __init__(self, path, value, metaprops):
        self.path = path
        self.value = value
        self.metaprops = metaprops
    
    def validvalues(self):
        opts = []
        val = self.metaprops.get("val")
        for opt in val:
            opts.append(KeyValue(key=opt, value=opt))
        return opts

    def valueType(self):
        return self.metaprops.get("type")

    def validate(self):
        # if meta properties is empty, then the property is unknown
        if self.metaprops == None:
            raise UnsupportedVssPropertyError("Unknown Covesa Path" + self.path)

        val = self.metaprops.get("val")
        if val != None:
            if self.value not in val:
                raise InvalidVssValueError("Invalid value supplied " + self.value)

### Write code for the new module here and import it from agent.py.
class UnsupportedVssPropertyError(Exception):
    pass

class InvalidVssValueError(Exception):
    pass

class SdvLinkRequest(Model):
    path: str = Field(description="Direct Path to Covesa VSS Value")
    value: str =  Field(description="Value for Covesa VSS Property")    
    driverId: str = Field(description="Driver/Vehicle ID for which the message is intended") 

class SdvLinkSetRequest(Model):
    path: str = Field(description="Direct Path to Covesa VSS Value")
    value: str =  Field(description="Value for Covesa VSS Property")    
    driverId: str = Field(description="Driver/Vehicle ID for which the message is intended")    

class SdvLinkGetRequest(Model):
    path: str = Field(description="Path to Covesa VSS Value")
    driverId: str = Field(description="Driver/Vehicle ID for which the message is intended")

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
    

sdvlink = Protocol()

# Handle Registration Request
@sdvlink.on_message(model=SdvLinkAgentRegistrationRequest)
async def handle_registration_message(ctx: Context, sender: str, msg: SdvLinkAgentRegistrationRequest):    
    ctx.logger.info(f"Registering {msg.driverId} of address {msg.agent_address}.")   
    remote_agents = ctx.storage.get("remote_agents")

    if remote_agents.get(msg.driverId) != msg.agent_address:
        ctx.logger.info(f"Overriding Driver ID {msg.driverId} with new agent address {msg.agent_address}")

    remote_agents[msg.driverId] = msg.agent_address
    ctx.storage.set("remote_agents", remote_agents)


# Handle SDV Get Request from DeltaV
@sdvlink.on_message(model=SdvLinkGetRequest, replies={UAgentResponse})
async def handle_chat_get_request(ctx: Context, sender: str, msg: SdvLinkGetRequest):    
    ctx.logger.info(f"Got Get Request {msg}")
    path = msg.path
    driverId = msg.driverId

    remote_agents = ctx.storage.get("remote_agents")
    ra = remote_agents.get(driverId)
    if ra == None:
        returnMsg = f"Unable to send message. Driver ID {driverId} is unknown/unregistered"
        ctx.logger.info(returnMsg)
        await ctx.send(sender, UAgentResponse(message=returnMsg, type=UAgentResponseType.FINAL))    
        return

    ctx.logger.info(f"Send message to remote agent {ra}")
    remoteMsg = SdvLinkGetRemoteRequest(
        path=path, 
        driverId=driverId 
    )
    await ctx.send(ra, remoteMsg)
    

# Handle SDV Set Request from DeltaV
@sdvlink.on_message(model=SdvLinkSetRequest, replies={UAgentResponse})
async def handle_chat_set_request(ctx: Context, sender: str, msg: SdvLinkSetRequest):    
    ctx.logger.info(f"Got Set Request {msg}")
    path = msg.path
    value = msg.value
    driverId = msg.driverId

    remote_agents = ctx.storage.get("remote_agents")
    ra = remote_agents.get(driverId)
    if ra == None:
        returnMsg = f"Unable to send message. Driver ID {driverId} is unknown/unregistered"
        ctx.logger.info(returnMsg)
        await ctx.send(sender, UAgentResponse(message=returnMsg, type=UAgentResponseType.FINAL))    
        return
    meta = VssMeta(path, value, ctx.storage.get("vssmeta").get(path))
    try:
        ctx.logger.info("Validating VSS Request Properties")
        meta.validate()
    except UnsupportedVssPropertyError as err:
        print(err)
        returnMsg = "Sorry. The given Covesa VSS Path " + path + " is not known or is unsupported"
        ctx.logger.info(returnMsg)   
        await ctx.send(sender, UAgentResponse(message=returnMsg, type=UAgentResponseType.FINAL))    
        return

    except InvalidVssValueError as err:
        ctx.logger.info(err)   
        await ctx.send(sender, UAgentResponse(options=meta.validvalues(), type=UAgentResponseType.SELECT_FROM_OPTIONS))
        return

    ctx.logger.info(f"Send message to remote agent {ra}")
    remoteMsg = SdvLinkRemoteSetRequest(
        path=path, 
        value=value,
        driverId=driverId, 
        valueType=meta.valueType()
    )

    await ctx.send(ra, remoteMsg)
    await ctx.send(sender, UAgentResponse(message="Successfully sent Covesa request to SDV", type=UAgentResponseType.FINAL))    

agent.include(sdvlink, publish_manifest=True) 
