import uvicorn  
import json  
from typing import Annotated  
from fastapi import Depends, FastAPI, Form, HTTPException, Request, BackgroundTasks  
from fastapi.responses import HTMLResponse  
from sqlalchemy import create_engine, select  
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session, sessionmaker  
from starlette.templating import Jinja2Templates  
import aio_pika  
from contextlib import asynccontextmanager  
import asyncio  

# Настройка подключения к базе данных SQLite  
DATABASE_URL = "sqlite:///./test.db"  
engine = create_engine(  
    DATABASE_URL,  
    connect_args={"check_same_thread": False}  
)  
  
# Инициализация шаблонов Jinja2  
templates = Jinja2Templates(directory="template")  
  
# Глобальная переменная для подключения к RabbitMQ  
rabbit_connection = None  
rabbit_channel = None  
  
  
@asynccontextmanager  
async def lifespan(app: FastAPI):  
    # Подключение к RabbitMQ при запуске приложения  
    global rabbit_connection, rabbit_channel  
    try:  
        rabbit_connection = await aio_pika.connect_robust("amqp://localhost:5672/")  
        rabbit_channel = await rabbit_connection.channel()  
        # Объявляем очередь  
        await rabbit_channel.declare_queue("task_queue", durable=True)  
        print("Connected to RabbitMQ")  
        yield  
    finally:  
        # Закрытие подключения при остановке приложения  
        if rabbit_connection:  
            await rabbit_connection.close()  
            print("Disconnected from RabbitMQ")  
  
  
# Создание FastAPI приложения с обработчиком жизненного цикла  
app = FastAPI(lifespan=lifespan)  
  
# Создание фабрики сессий для работы с базой данных  
new_session = sessionmaker(autocommit=False, autoflush=False, bind=engine)  
  
  
def get_session():  
    with new_session() as session:  
        yield session  
  
  
SessionDep = Annotated[Session, Depends(get_session)]  
  
  
class Base(DeclarativeBase):  
    pass  
  
  
class TaskModel(Base):  
    __tablename__ = "tasks"  
    id: Mapped[int] = mapped_column(primary_key=True)  
    name: Mapped[str]  
    description: Mapped[str]  
    status: Mapped[str]  
  
  
Base.metadata.create_all(bind=engine)  
  
  
async def send_to_rabbitmq(task_data: dict):  
    """Асинхронная функция для отправки сообщения в RabbitMQ"""  
    try:  
        message_body = json.dumps(task_data).encode()  
        message = aio_pika.Message(  
            message_body,  
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT  
        )  
        await rabbit_channel.default_exchange.publish(  
            message,  
            routing_key="task_queue"  
        )  
        print(f"Sent task to RabbitMQ: {task_data}")  
    except Exception as e:  
        print(f"Error sending to RabbitMQ: {e}")  
  
  
@app.get("/", response_class=HTMLResponse, tags=["Задачи"], summary="HTML форма для создания задач")  
async def read_form(request: Request):  
    return templates.TemplateResponse("form.html", {"request": request})  
  
  
@app.post("/submit", tags=["Задачи"], summary="Создание новой задачи")  
async def add_task(  
        background_tasks: BackgroundTasks,  
        name: str = Form(),  
        description: str = Form(),  
        status: str = Form(),  
        db: Session = Depends(get_session)  
):  
    new_task = TaskModel(  
        name=name,  
        description=description,  
        status=status  
    )  
    db.add(new_task)  
    db.commit()  
    db.refresh(new_task)  
  
    # Подготавливаем данные для отправки в RabbitMQ  
    task_data = {  
        "id": new_task.id,  
        "name": new_task.name,  
        "description": new_task.description,  
        "status": new_task.status  
    }  
  
    # Добавляем задачу отправки в RabbitMQ в фоновые задачи  
    background_tasks.add_task(send_to_rabbitmq, task_data)  
  
    return {"Статус": "Данные успешно добавлены"}  
  
  
@app.get("/task", tags=["Задачи"], summary="Получение всех задач")  
def get_task(session: SessionDep):  
    query = select(TaskModel)  
    result = session.execute(query)  
    return result.scalars().all()  
  
  
@app.delete("/task/{id}", tags=["Задачи"], summary="Удаление задачи")  
def del_task(id: int, db: Session = Depends(get_session)):  
    task = db.query(TaskModel).filter(TaskModel.id == id).first()  
    if task is None:  
        raise HTTPException(status_code=404, detail="Task not found")  
    db.delete(task)  
    db.commit()  
    return {"message": "ok"}  
  
  
if __name__ == "__main__":  
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
