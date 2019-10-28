ARG INSTALL_PATH=/opt/broadway

FROM python:3.6.9-alpine

ADD requirements-test.txt ${INSTALL_PATH}

RUN apk add --no-cache git && \
    pip install -r ${INSTALL_PATH}/requirements-test.txt

ADD tests ${INSTALL_PATH}/tests

ENV PYTHONPATH "${PYTHONPATH}:${INSTALL_PATH}"

ENTRYPOINT ["py.test", "-v", "tests/integration"]
CMD []

