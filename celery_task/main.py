import os
import time
from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Mapped, mapped_column, DeclarativeBase
import redis

# Получение параметров подключения из переменных окружения
broker = os.environ.get('CELERY_BROKER', 'redis://localhost:6379/0')
backend = os.environ.get('CELERY_BACKEND', 'redis://localhost:6379/0')

# Инициализация Celery приложения
celery = Celery("main", broker=broker, backend=backend)

# Подключение к БД первого микросервиса
DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///../task_API/test.db')
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    """Базовый класс для моделей SQLAlchemy"""
    pass


class Task(Base):
    """Модель задачи в базе данных"""
    
    __tablename__ = "tasks"
    
    # Поля таблицы tasks
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

        # Запускаем обработку для каждой найденной задачи
        for task in pending_tasks:
            process_task.delay(task.id)

    finally:
        # Всегда закрываем сессию
        session.close()


@celery.task
def process_task(task_id):
    """Функции для обработки конкретной задачи"""
    
    session = SessionLocal()
    task = None  # Инициализация переменной для обработки исключений
    
    try:
        # Получаем задачу по ID
        task = session.query(Task).get(task_id)
        if not task:
            return  # Если задача не найдена, выходим

        # Меняем статус на 'В работе'
        task.status = 'В работе'
        session.commit()

        # Логика обработки задачи
        print(f"Задача взята в работу: {task.name}")

        # Имитация обработки (5 секунд)
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
        # Пробрасываем исключение дальше для логирования Celery
        raise e
    finally:
        # Всегда закрываем сессию
        session.close()


# Настройка периодических задач для Celery Beat
celery.conf.beat_schedule = {
    'process-tasks-every-30-seconds': {
        'task': 'process_pending_tasks',  
        'schedule': 30.0,  
    },
}
