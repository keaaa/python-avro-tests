from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient


async def send_to_event_hub(conn_str, bytes):
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str=conn_str)
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()
        event = EventData(bytes)

        # Add events to the batch.
        event_data_batch.add(event)

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)
