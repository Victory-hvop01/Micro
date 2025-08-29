from celery import Celery
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.orm import sessionmaker, Mapped, mapped_column, DeclarativeBase
import redis
import os
broker = os.environ.get('CELERY_BROKER', 'redis://localhost:6379/0')
backend = os.environ.get('CELERY_BACKEND', 'redis://localhost:6379/0')

celery = Celery("main", broker=broker, backend=backend)

# Подключение к БД первого микросервиса
DATABASE_URL = 'sqlite:///../Micro1/test.db'
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


@celery.task(name='process_pending_tasks')
def process_pending_tasks():
    """Периодическая задача для проверки новых задач в БД"""
    session = SessionLocal()
    try:
        # Ищем задачи со статусом 'Ожидание'
        pending_tasks = session.query(Task).filter(Task.status == 'Ожидание').all()

        for task in pending_tasks:
            # Запускаем обработку для каждой задачи
            process_task.delay(task.id)

    finally:
        session.close()


@celery.task
def process_task(task_id):
    """Задача для обработки конкретной задачи"""
    session = SessionLocal()
    try:
        task = session.query(Task).get(task_id)
        if not task:
            return

        # Меняем статус на 'В работе'
        task.status = 'В работе'
        session.commit()

        # Здесь логика обработки задачи
        print(f"Задача взята в работу: {task.name}")
        #print(f"Description: {task.description}")

        # Имитация обработки
        import time
        time.sleep(5)

        # Меняем статус на 'Готово'
        task.status = 'Готово'
        session.commit()
        print(f"Задача сделана: {task.name}")

    except Exception as e:
        # В случае ошибки меняем статус на 'Ошибка обработки'
        if task:
            task.status = 'Ошибка обработки'
            session.commit()
        raise e
    finally:
        session.close()


celery.conf.beat_schedule = {
    'process-tasks-every-30-seconds': {
        'task': 'process_pending_tasks',
        'schedule': 30.0,
    },
}


