FROM agrigorev/zoomcamp-model:mlops-3.9.7-slim

RUN pip install -U pip
RUN pip install pipenv

WORKDIR /app

COPY ["Pipfile", "Pipfile.lock", "./"]

RUN pipenv install --system --deploy

COPY 'predict.py' './predict.py'

ENV YEAR='2021'
ENV MONTH='03'

ENTRYPOINT python predict.py ${YEAR} ${MONTH}

