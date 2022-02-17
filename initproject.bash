#!/bin/bash
set -e

# Load environment variables for postactivate
. ./env.config

PROJECT_NAME="aquarius_bribes"

if [ -z "$DATABASE_URL" ]; then
    echo "declare environment variables in env.config"
    exit 0
fi

# Initialize virtualenvwrapper
source "/usr/local/bin/virtualenvwrapper.sh"
WORKON_HOME=$HOME/.virtualenvs


# Create virtualenv
if [ -d "$WORKON_HOME/$PROJECT_NAME" ]; then
    echo "$PROJECT_NAME virtualenv already exists."
    workon $PROJECT_NAME || true
else
    mkvirtualenv $PROJECT_NAME -p python3 || true
fi

# Write variables to postactivate
postactivate_file_path="$WORKON_HOME/$PROJECT_NAME/bin/postactivate"
printf 'export DATABASE_URL="%s"\n\ncd "%s"' "$DATABASE_URL" "$(pwd)" > "$postactivate_file_path"

# Install requirements
pip install -r requirements/dev.txt

# We need to reload virtualenv
deactivate
workon $PROJECT_NAME || true

# Make manage.py executable
chmod 755 manage.py

echo "RUNNING INITIAL MIGRATIONS"
python manage.py migrate
echo "CREATE SUPERUSER"
python manage.py createsuperuser

deactivate
exit 0
