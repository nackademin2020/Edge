import time
import os
import sys
import asyncio
from six.moves import input
import pandas
import joblib

from edge_module_client import EdgeModuleClient

module_client = None
output_name = ""
model_path = "model.pkl"
model = None

def twin_callback(twin_properties):
    print(f"Twin callback with: {twin_properties}")
    return twin_properties

async def input_callback(message):
    global module_client
    global output_name
    global model

    print(f"Message received: {message}")

    input_df = pandas.DataFrame([[message["machine"]["temperature"],message["machine"]["pressure"],message["ambient"]["temperature"],message["ambient"]["humidity"]]])
    pred = model.predict(input_df)
    print(f"Prediction is {pred[0]}")

    if pred[0] == 1:
        message["anomaly"]=True
    else:
        message["anomaly"]=False

    await module_client.send_json_message_to_output(message, output_name, {"type": "simulatedtemperature"})

async def main():
    global module_client
    global output_name
    global model
    global model_path

    try:
        if not sys.version >= "3.5.3":
            raise Exception( "The sample requires python 3.5.3+. Current version of Python: %s" % sys.version )
        print ( "IoT Hub Client for Python" )

        input_name = os.getenv("INPUT")
        output_name = os.getenv("OUTPUT")

        # The client object is used to interact with your Azure IoT hub.
        module_client = EdgeModuleClient(twin_callback, input_callback, input_name)
        # connect the client.
        await module_client.start()

        model = joblib.load(model_path)

        # define behavior for halting the application
        def stdin_listener():
            while True:
                try:
                    selection = input("Press Q to quit\n")
                    if selection == "Q" or selection == "q":
                        print("Quitting...")
                        break
                except:
                    time.sleep(10)

        # Run the stdin listener in the event loop
        loop = asyncio.get_event_loop()
        user_finished = loop.run_in_executor(None, stdin_listener)

        # Wait for user to indicate they are done listening for messages
        await user_finished

        # Finally, disconnect
        await module_client.disconnect()

    except Exception as e:
        print ( "Unexpected error %s " % e )
        raise

if __name__ == "__main__":
    asyncio.run(main())