ARG INSTALL_PATH=/opt/broadway

FROM python:3.6.9-alpine

ADD requirements.txt ${INSTALL_PATH}

RUN apk add --no-cache git && \
    pip install -r ${INSTALL_PATH}/requirements.txt

ADD broadway ${INSTALL_PATH}/broadway

ENV PYTHONPATH "${PYTHONPATH}:${INSTALL_PATH}"

ENTRYPOINT ["python", "-m", "broadway.api"]
CMD []
