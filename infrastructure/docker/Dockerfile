FROM python:3.10-slim

# make sure it doesnt fail if the docker file doesnt know the git commit
ARG GIT_PYTHON_REFRESH=quiet

#RUN apt-get -qq update \
#    && apt-get -qq install -y --no-install-recommends \
#        libpq-dev \
#    && apt-get autoclean && apt-get autoremove \
#    > /dev/null


# Install poetry.
RUN pip install "poetry==1.3.2" --no-cache-dir

# Copy files.
COPY pyproject.toml app/pyproject.toml
COPY README.md app/README.md
COPY poetry.lock app/poetry.lock

# copy library files
COPY pvconsumer/ app/pvconsumer/

# change to app folder
WORKDIR /app

# Install python requirements.
RUN poetry install --witout dev --no-cache

# install library
RUN pip install -e .

CMD ["python", "-u","pvconsumer/app.py"]
