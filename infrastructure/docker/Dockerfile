FROM python:3.10-slim

# make sure it doesnt fail if the docker file doesnt know the git commit
ARG GIT_PYTHON_REFRESH=quiet

#RUN apt-get -qq update \
#    && apt-get -qq install -y --no-install-recommends \
#        libpq-dev \
#    && apt-get autoclean && apt-get autoremove \
#    > /dev/null

# copy files
COPY setup.py app/setup.py
COPY README.md app/README.md
COPY requirements.txt app/requirements.txt

# install requirements
RUN pip install -r app/requirements.txt

# copy library files
COPY pvconsumer/ app/pvconsumer/

# change to app folder
WORKDIR /app

# install library
RUN pip install -e .

CMD ["python", "-u","pvconsumer/app.py"]
