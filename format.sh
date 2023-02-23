#
# Format the codebase.
#

SRC="pvconsumer tests"

# poetry run ruff --fix $SRC
poetry run black $SRC
poetry run isort $SRC
