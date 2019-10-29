ARG INSTALL_PATH=/opt/broadway

FROM python:3.6.9-alpine

ADD requirements-test.txt ${INSTALL_PATH}

# python3-dev, gcc, and build-base is required for building some python packages (typed-ast in particular)
RUN apk add --no-cache git python3-dev gcc build-base && \
    pip install -r ${INSTALL_PATH}/requirements-test.txt

ADD tests ${INSTALL_PATH}/tests

ENV PYTHONPATH "${PYTHONPATH}:${INSTALL_PATH}"

ENTRYPOINT ["py.test", "-v", "tests/integration"]
CMD []

