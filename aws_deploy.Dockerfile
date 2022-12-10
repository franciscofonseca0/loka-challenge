FROM python:3.9

RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -
RUN apt-get install -y nodejs
RUN npm install -g aws-cdk

WORKDIR /app
COPY ./aws_cdk .
RUN pip install -r requirements.txt