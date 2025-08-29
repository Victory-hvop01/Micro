from typing import Annotated
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session, sessionmaker
from fastapi import Depends, FastAPI, Form, HTTPException, Request
from sqlalchemy import select
from fastapi.responses import HTMLResponse
import uvicorn
from sqlalchemy import create_engine
from starlette.templating import Jinja2Templates


DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

templates = Jinja2Templates(directory="new")
app = FastAPI()
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


# Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)


@app.get("/", response_class=HTMLResponse, tags=["Задачи"], summary="HTML форма")
async def read_form(request: Request):
    return templates.TemplateResponse("form.html", {"request": request})


@app.post("/submit", tags=["Задачи"], summary="Запись задач")
async def add_task(name: str = Form(), description: str = Form(), status: str = Form(), db: Session = Depends(get_session)):
    new_task = TaskModel(
        name=name,
        description=description,
        status=status
    )
    db.add(new_task)
    db.commit()
    '''connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='task_notifications', exchange_type='fanout')

    channel.basic_publish(
        exchange='task_notifications',
        routing_key='',
        body=json.dumps({'action': 'new_task', 'task_id': new_task.id})
    )
    connection.close()'''
    return {"Статус": "Данные успешно добавлены"}


@app.get("/task", tags=["Задачи"], summary="Получение задач")
def get_task(session: SessionDep):
    query = select(TaskModel)
    result = session.execute(query)
    return result.scalars().all()


@app.delete("/task/{id}", tags=["Задачи"], summary="Удаление задач")
def del_task(id: int = TaskModel, db: Session = Depends(get_session)):
    user = db.query(TaskModel).filter(TaskModel.id == id).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    db.delete(user)
    db.commit()
    return {"messege": "ok"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)