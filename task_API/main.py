import uvicorn
from typing import Annotated
from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session, sessionmaker
from starlette.templating import Jinja2Templates

# Настройка подключения к базе данных SQLite
DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(
    DATABASE_URL, 
    connect_args={"check_same_thread": False}  # Требуется для SQLite
)

# Инициализация шаблонов Jinja2
templates = Jinja2Templates(directory="template")

# Создание FastAPI приложения
app = FastAPI()

# Создание фабрики сессий для работы с базой данных
new_session = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_session():
    """
    Зависимость для получения сессии базы данных.
    Гарантирует правильное закрытие сессии после использования.
    """
    with new_session() as session:
        yield session


# Аннотированный тип для удобного использования зависимости сессии
SessionDep = Annotated[Session, Depends(get_session)]


class Base(DeclarativeBase):
    """Базовый класс для всех моделей SQLAlchemy"""
    pass


class TaskModel(Base):
    """Модель задачи в базе данных"""
    
    __tablename__ = "tasks"
    
    # Определение полей таблицы
    id: Mapped[int] = mapped_column(primary_key=True)  # Первичный ключ
    name: Mapped[str]                                  # Название задачи
    description: Mapped[str]                           # Описание задачи
    status: Mapped[str]                                # Статус задачи


# Создание таблиц в базе данных (раскомментируйте для сброса данных)
# Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)


@app.get(
    "/", 
    response_class=HTMLResponse, 
    tags=["Задачи"], 
    summary="HTML форма для создания задач"
)
async def read_form(request: Request):
    """Отображает HTML форму для создания новых задач"""
    return templates.TemplateResponse("form.html", {"request": request})


@app.post("/submit", tags=["Задачи"], summary="Создание новой задачи")
async def add_task(
    name: str = Form(),
    description: str = Form(),
    status: str = Form(),
    db: Session = Depends(get_session)
):
    """Создает новую задачу в базе данных"""
    new_task = TaskModel(
        name=name,
        description=description,
        status=status
    )
    db.add(new_task)
    db.commit()
    return {"Статус": "Данные успешно добавлены"}


@app.get("/task", tags=["Задачи"], summary="Получение всех задач")
def get_task(session: SessionDep):
    """Возвращает список всех задач из базы данных"""
    query = select(TaskModel)
    result = session.execute(query)
    return result.scalars().all()


@app.delete("/task/{id}", tags=["Задачи"], summary="Удаление задачи")
def del_task(id: int, db: Session = Depends(get_session)):
    """Удаляет задачу по указанному идентификатору"""
    task = db.query(TaskModel).filter(TaskModel.id == id).first()
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    db.delete(task)
    db.commit()
    return {"message": "ok"} 


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
