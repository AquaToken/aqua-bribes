Installing Django
=================

To install Django in the new virtual environment, run the following command::

    pip install django>=2.1

Creating your project
=====================

To create a new Django project called '**new_project**' using
template, run the following commands:

    git clone https://bitbucket.org/razortheory/new-django-aws 
    django-admin.py startproject --template=./new-django-aws --extension=py,md,conf,txt,base_env,bash aquarius_bribes
    pip install -r requirements.txt

Creating AWS instances
======================

You must create EC2, RDS instances and S3 bucket for staging and production.

1. For EC2 instance ports must be open for SSH and HTTP protocol. For RDS - 5432.
2. Database name must be same as project name.
3. For EC2 it would be better to associate Elastic IP.

Set deploy settings
===================

Use django deployer (https://bitbucket.org/razortheory/django-deployer) to setup & deploy project
