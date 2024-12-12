from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config
from colorama import Fore, Style, init

# Ініціалізація кольорового логування
init(autoreset=True)

# Створення клієнта Kafka
try:
    print(f"{Fore.CYAN}Connecting to Kafka Admin Client...")
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )
    print(f"{Fore.GREEN}Connected to Kafka Admin Client successfully.")
except Exception as e:
    print(f"{Fore.RED}Failed to connect to Kafka Admin Client: {e}")
    exit(1)

# Визначення імен топіків із врахуванням my_name
my_name = "ivan"
topic_name_in = f"{my_name}_sensors_data"
alerts_topic_name = f"{my_name}_alerts"
num_partitions = 2
replication_factor = 1

# Створення топіків
new_topics = [
    NewTopic(name=topic_name_in, num_partitions=num_partitions, replication_factor=replication_factor),
    NewTopic(name=alerts_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
]

try:
    print(f"{Fore.CYAN}Creating topics: {topic_name_in} and {alerts_topic_name}...")
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"{Fore.GREEN}Topics '{topic_name_in}' and '{alerts_topic_name}' created successfully.")
except Exception as e:
    if "TopicExistsException" in str(e):
        print(f"{Fore.YELLOW}Topics already exist: {e}")
    else:
        print(f"{Fore.RED}An error occurred while creating topics: {e}")
finally:
    print(f"{Fore.CYAN}Closing Kafka Admin Client...")
    admin_client.close()
    print(f"{Fore.GREEN}Kafka Admin Client closed successfully.")
