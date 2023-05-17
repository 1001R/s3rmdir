FROM scratch
COPY s3rmdir /
ENTRYPOINT [ "/s3rmdir" ]