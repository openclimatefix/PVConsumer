#
# Format the codebase.
#

SRC="pvconsumer tests"

poetry run black $SRC
poetry run isort $SRC
