1. Install pre-requisites
=========================

Virtualenv + pipenv
----------
Standard installation.

PostgreSQL
----------
Standard installation.



# Standard project initialization
## 1. Create virtual environment


1. Clone repository: ``git clone https://bitbucket.org/razortheory/aquarius_bribes.git``
2. Install requirements ``pipenv install --dev``
3. Enter pipenv shell ``pipenv shell``
3. Edit ``$VIRTUAL_ENV/bin/postactivate`` to contain the following lines:

        export DATABASE_URL=postgres://username:password@localhost/dbname
        export DEV_ADMIN_EMAIL=your@email.com

4. Exit from pipenv shell ``deactivate``


## 2. Database

1. Create database table:

        psql -Uyour_psql_user
        CREATE DATABASE aquarius_bribes;

2. Migrations: ``pipenv run manage.py migrate``
3. Create admin: ``pipenv run manage.py createsuperuser``
4. Run the server ``pipenv run manage.py runserver``
