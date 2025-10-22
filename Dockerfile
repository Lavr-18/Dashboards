# Использование официального образа Python
FROM python:3.11-slim

# Установка рабочей директории внутри контейнера
WORKDIR /usr/src/app

# Копирование файла с зависимостями и установка зависимостей
# Используем --no-cache-dir для уменьшения размера образа
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование всего остального кода в рабочую директорию
COPY . .

# Присвоение переменной окружения, чтобы Python не буферизировал stdout/stderr
ENV PYTHONUNBUFFERED 1

# Команда, которая будет выполняться при запуске контейнера
# (В продакшене чаще запускают telegram_bot.py)
CMD ["python", "telegram_bot.py"]