FROM python:3.6.9-alpine

ARG INSTALL_PATH=/opt/broadway
RUN mkdir -p ${INSTALL_PATH}

ADD requirements.txt ${INSTALL_PATH}

RUN apk add --no-cache git && \
    pip install -r ${INSTALL_PATH}/requirements.txt

ADD broadway ${INSTALL_PATH}/broadway

ENV PYTHONPATH "${PYTHONPATH}:${INSTALL_PATH}"

WORKDIR /srv/cs241/broadway-grader
ENTRYPOINT ["python", "-m", "broadway.grader"]
CMD []
