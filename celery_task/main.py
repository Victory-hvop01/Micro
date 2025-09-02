import os  
import json  
import time  
import pika  
from sqlalchemy import create_engine, select  
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column  
from datetime import datetime  
from celery import Celery  
  
# Настройки подключения  
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')  
RABBITMQ_QUEUE = os.environ.get('RABBITMQ_QUEUE', 'task_queue')  
DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///../task_API/test.db')  
  
# Инициализация Celery для фоновых задач  
celery_broker = os.environ.get('CELERY_BROKER', 'redis://localhost:6379/0')  
celery_backend = os.environ.get('CELERY_BACKEND', 'redis://localhost:6379/0')  
  
celery_app = Celery('main', broker=celery_broker, backend=celery_backend)  
  
# Подключение к БД  
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})  
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)  
  
  
class Base(DeclarativeBase):  
    pass  
  
  
class Task(Base):  
    __tablename__ = "tasks"  
  
    id: Mapped[int] = mapped_column(primary_key=True)  
    name: Mapped[str]  
    description: Mapped[str]  
    status: Mapped[str]  
  
  
  
# Создание таблиц  
Base.metadata.create_all(bind=engine)  
  
  
@celery_app.task  
def process_long_task(task_id):  
    """Фоновая обработка долгих задач через Celery"""  
    session = SessionLocal()  
    try:  
          
        task = session.get(Task, task_id)  
        if not task:  
            print(f"Долгая задача с ID {task_id} не найдена")  
            return  
  
        print(f"Начата фоновая обработка долгой задачи: {task.name}")  
  
        # Меняем статус  
        task.status = 'В работе'  
        session.commit()  
  
        # Имитация долгой обработки (30 секунд)  
        time.sleep(30)  
  
        # Завершаем обработку  
        task.status = 'Готово'  
        session.commit()  
  
        print(f"Долгая задача завершена: {task.name}")  
  
    except Exception as e:  
        print(f"Ошибка при фоновой обработке задачи: {e}")  
  
    finally:  
        session.close()  
  
  
def process_normal_task(task_data):  
    """Обработка обычных задач (не долгих)"""  
    session = SessionLocal()  
    try:  
        )  
        task = session.get(Task, task_data['id'])  
        if not task:  
            print(f"Задача с ID {task_data['id']} не найдена")  
            return False  
  
        print(f"Начата обработка обычной задачи: {task.name}")  
  
        # Меняем статус  
        task.status = 'В работе'  
        session.commit()  
  
        # Имитация обработки (3 секунды)  
        time.sleep(3)  
  
        # Завершаем обработку  
        task.status = 'Готово'  
        session.commit()  
  
        print(f"Обычная задача завершена: {task.name}")  
        return True  
  
    except Exception as e:  
        print(f"Ошибка при обработке обычной задачи: {e}")  
        if task_data:  
            task_data['status'] = 'Ошибка'  
            session.commit()  
        return False  
    finally:  
        session.close()  
  
  
def on_message_received(channel, method, properties, body):  
    """Обработчик входящих сообщений из RabbitMQ"""  
    try:  
        # Парсим данные задачи  
        task_data = json.loads(body)  
        print(f"Получена задача: {task_data}")  
  
        # Определяем тип задачи и обрабатываем соответствующим образом  
        if task_data.get('status') == 'Ожидание долгой задачи':  
            # Для долгих задач отправляем в Celery  
            process_long_task.delay(task_data['id'])  
            print(f"Долгая задача отправлена в Celery: {task_data['id']}")  
        else:  
            # Обычные задачи обрабатываем сразу  
            success = process_normal_task(task_data)  
            if success:  
                print(f"Обычная задача обработана: {task_data['id']}")  
  
        # Подтверждаем получение сообщения  
        channel.basic_ack(delivery_tag=method.delivery_tag)  
  
    except Exception as e:  
        print(f"Ошибка при обработке сообщения: {e}")  
        # Отказываемся от сообщения при ошибке  
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  
  
  
def start_rabbitmq_consumer():  
    """Запуск потребителя RabbitMQ"""  
    try:  
        # Подключаемся к RabbitMQ  
        connection_params = pika.ConnectionParameters(  
            host=RABBITMQ_HOST,  
            heartbeat=600,  
            blocked_connection_timeout=300  
        )  
  
        connection = pika.BlockingConnection(connection_params)  
        channel = connection.channel()  
  
        # Объявляем очередь  
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)  
  
        # Настраиваем QoS  
        channel.basic_qos(prefetch_count=1)  
  
        # Начинаем потребление сообщений  
        channel.basic_consume(  
            queue=RABBITMQ_QUEUE,  
            on_message_callback=on_message_received  
        )  
  
        print("Микросервис запущен и ожидает задачи из RabbitMQ...")  
        channel.start_consuming()  
  
    except Exception as e:  
        print(f"Ошибка подключения к RabbitMQ: {e}")  
        # Переподключаемся через 10 секунд  
        time.sleep(10)  
        start_rabbitmq_consumer()  
  
  
if __name__ == "__main__":  
    start_rabbitmq_consumer()
