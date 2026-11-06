FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CTI_CONFIG=/app/config/project.json \
    CTI_DATABASE=/data/inbox.sqlite

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pytest.ini .
COPY comment_to_inbox ./comment_to_inbox
COPY tests ./tests
COPY config ./config

CMD ["python", "-m", "comment_to_inbox", "serve"]
