import urllib.request, json, time
# The demo payment service reads PULSE_INCIDENT at process startup.
# For an already-running container, use:
#   docker compose stop payment
#   docker compose run -d -e PULSE_INCIDENT=1 --service-ports payment
# Then generate traffic.
print("Demo incident instructions:")
print("1. docker compose stop payment")
print("2. docker compose run -d --name pulse-payment-incident -e KAFKA_BROKERS=redpanda:9092 -e PULSE_INCIDENT=1 -p 7001:7001 payment")
print("3. python scripts/generate_traffic.py --seconds 90")
print("4. Open http://localhost:3000")
