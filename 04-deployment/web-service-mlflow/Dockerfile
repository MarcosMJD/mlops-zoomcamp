FROM python:3.9.7-slim

RUN pip install -U pip
RUN pip install pipenv 

WORKDIR /app

COPY [ "Pipfile", "Pipfile.lock", "./" ]
COPY [ "./credentials/credentials", "./credentials/config", "/root/.aws/" ]

RUN pipenv install --system --deploy

COPY [ "predict.py", "./" ]

EXPOSE 9000

# ENTRYPOINT ["bash"]
ENTRYPOINT [ "gunicorn", "--bind=0.0.0.0:9000", "predict:app" ]