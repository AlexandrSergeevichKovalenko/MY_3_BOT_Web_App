# Используем базовый образ Python
FROM python:3.10-slim

# Устанавливаем только необходимые системные зависимости
RUN apt-get update && apt-get install -y \
    build-essential \
    espeak-ng \
    libespeak1 \
    libespeak-ng1 \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем pip и зависимости для PyQt5
RUN pip install --upgrade pip

#pydub требует ffmpeg на уровне системы! В Docker-файле нужно будет добавить
RUN apt-get update && apt-get install -y ffmpeg

# Устанавливаем остальные зависимости из requirements.txt
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --progress-bar off --retries 10 --timeout 120 -r requirements.txt
RUN pip install --no-cache-dir --progress-bar off --retries 10 --timeout 120 \
  https://github.com/explosion/spacy-models/releases/download/de_core_news_sm-3.7.0/de_core_news_sm-3.7.0-py3-none-any.whl

# Копируем исходный код
COPY . .


# Указываем переменные окружения
ENV YOUTUBE_API_KEY=""
ENV OPENAI_API_KEY=""
ENV API_KEY_NEWS=""
ENV DATABASE_URL_RAILWAY=""
ENV TELEGRAM_DeepSeek_BOT_TOKEN=""
ENV CLAUDE_API_KEY=""
ENV GOOGLE_APPLICATION_CREDENTIALS=""
ENV LIVEKIT_URL=""
ENV LIVEKIT_API_KEY=""
ENV LIVEKIT_API_SECRET=""



CMD ["python", "bot_3.py"]
