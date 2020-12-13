from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message
import asyncio
import json
import datetime
import logging

class EdgeModuleClient:
  """
  Responsible for all communication with edge hub. Creates a new module client from the edge environment
  Args:
    twin_update_callback (callable) - Function to be called when an update for twin desired properties is received. Input will be a dictonary with desired properties
  """
  listeners = None
  module_client = None
  def __init__(self, twin_update_callback, input_callback, input_name):
    self.module_client = IoTHubModuleClient.create_from_edge_environment()
    self.twin_update_callback = twin_update_callback
    self.input_callback = input_callback
    self.input_name = input_name

  def __del__(self):
    if self.listeners is not None:
      self.listeners.cancel()
    if self.module_client is not None:
      self.module_client.disconnect()

  async def start(self):
    """
      Initialize connection to IoT Hub. Reads the current desired properties for the device and reports back to IoT Hub. Shedules listener for twin updates
    """
    await self.module_client.connect()

    desired_properties = await self._get_twin_desired_properties()
    properties_to_report = self.twin_update_callback(desired_properties)
    await self._report_properties(properties_to_report)

    # Schedule task for C2D Listener
    self.listeners = asyncio.gather(self._twin_listener(), self._input_listener(self.input_name, self.input_callback))

  async def disconnect(self):
    self.listeners.cancel()
    await module_client.disconnect()

  async def send_json_message_to_output(self, payload, output_name, properties = {}):
    """
      Send a json payload to the module output. Routing of messages done in the deployment template
      
      Args:
        payload (dict): JSON serializable dictionary to be send
        output_name (str): The name of the output to publish the message to
        properties (:obj:´dict´, optional): Custom properties to add to the message. Will be added to message applicationProperties that can be queried for in message routing
    """
    message = Message(bytearray(json.dumps(payload), "utf8"), content_type = "application/json", content_encoding = "utf-8")
    message.custom_properties = properties

    await self.module_client.send_message_to_output(message, output_name)

  async def _twin_listener(self):
    """
      Listens to changes in module twin. Once a new update is available, the desired properties are polled and the twin_update_callback is called with the properties. Runs forever.
    """
    while True:
      try:
        desired_properties = await self.module_client.receive_twin_desired_properties_patch() #Blocking
        version = desired_properties.pop('$version')

        logging.info(f'Twin patch desired version {version}: {desired_properties}')

        properties_to_report = self.twin_update_callback(desired_properties)
        await self._report_properties(properties_to_report)

      except Exception as e:
        logging.exception(e)

  async def _report_properties(self, properties):
    """
      Reports properties to module twin
    """
    properties["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    await self.module_client.patch_twin_reported_properties(properties)

  async def _get_twin_desired_properties(self):
    """
      Polls the current desired properties from the module twin
    """
    twin = await self.module_client.get_twin()
    desired = twin.get('desired')
    # Remove version
    version = desired.pop('$version')

    logging.info(f'Twin desired version {version}: {desired}')

    return desired

  async def _input_listener(self, input_name, input_callback):
    try:
      while True:
        message = await self.module_client.receive_message_on_input(input_name)
        message_json = json.loads(message.data.decode('utf-8'))
        await input_callback(message_json)
    except Exception as e:
      logging.exception(e)