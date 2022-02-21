# syntax=docker/dockerfile:1

FROM golang:1.17-alpine

WORKDIR /app

COPY . ./

RUN go mod tidy

RUN go build -o /prog4-docker-test

EXPOSE 8000

CMD [ "/prog4-docker-test" ]