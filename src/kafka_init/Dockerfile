FROM python:3.13-bullseye

RUN mkdir /app
COPY . /app/

WORKDIR /app

RUN pip3 install uv~=0.10
RUN python3 -m uv pip install --system .
RUN touch /app/task.conf

CMD ["python3", "entrypoint.py"]