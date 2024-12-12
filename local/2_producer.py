from kafka import KafkaProducer
import json
import time
import random
from configs import kafka_config
from colorama import Fore, Style, init

# Ініціалізація кольорового виведення
init(autoreset=True)

my_name = "ivan"
topic_name_in = f"{my_name}_sensors_data"

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Генерація даних у нескінченному циклі
try:
    while True:
        data = {
            "id": random.randint(1, 100),
            "temperature": random.uniform(-50, 50),
            "humidity": random.uniform(0, 100),
            "timestamp": time.time()
        }
        producer.send(topic_name_in, value=data)

        # Кольорове логування
        print(
            f"{Fore.GREEN}Sent: {Style.BRIGHT}{data}{Style.RESET_ALL}"
        )

        time.sleep(1)  # Інтервал у 1 секунду між відправленням повідомлень
except KeyboardInterrupt:
    print(f"{Fore.YELLOW}Data generation interrupted by user.")

finally:
    producer.close()
    print(f"{Fore.RED}Kafka producer closed.")