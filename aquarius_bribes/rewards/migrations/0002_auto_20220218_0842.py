# Generated by Django 3.2.12 on 2022-02-18 08:42

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('rewards', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='payout',
            name='asset_code',
            field=models.CharField(default=None, max_length=12),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='payout',
            name='asset_issuer',
            field=models.CharField(default=None, max_length=56),
            preserve_default=False,
        ),
    ]