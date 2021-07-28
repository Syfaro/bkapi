FROM debian:buster-slim
EXPOSE 3000
COPY ./bkapi /bin/bkapi
CMD ["/bin/bkapi"]
