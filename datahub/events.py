from pubsub.bus import BUS

def publish_event(event_type: str, symbol: str | None = None, payload: dict | None = None, source: str = "datahub"):
    BUS.publish("events", {
        'type': event_type,
        'symbol': symbol,
        'payload': payload or {},
        'source': source
    })
