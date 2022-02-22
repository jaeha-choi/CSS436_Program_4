# syntax=docker/dockerfile:1

FROM golang:1.17-alpine

WORKDIR /prog-4

COPY . ./

RUN go mod tidy

RUN go build -o /prog-4-docker

EXPOSE 8000

CMD [ "/prog-4-docker" ]