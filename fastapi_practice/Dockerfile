#
FROM python:3.9

# 
WORKDIR /code

# 
COPY ./requirements.txt /code/requirements.txt

# 
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
RUN pip install fastapi
RUN pip install boto3
RUN pip install pyarrow
RUN pip install "uvicorn[standard]"

# 
COPY ./main.py /code/app/main.py

# 
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
