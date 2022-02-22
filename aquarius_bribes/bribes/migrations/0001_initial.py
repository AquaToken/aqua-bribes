# Generated by Django 3.2.12 on 2022-02-22 07:23

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Bribe',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('status', models.IntegerField(choices=[(0, 'Pending unlock time'), (1, 'Invalid bribe'), (2, 'Active bribe'), (3, 'Returned'), (4, 'Pending unlock time to return'), (5, 'Failed claim'), (6, 'Conversion failed'), (7, 'Failed return')])),
                ('message', models.TextField()),
                ('market_key', models.CharField(max_length=56)),
                ('sponsor', models.CharField(max_length=56)),
                ('amount', models.DecimalField(decimal_places=7, max_digits=20)),
                ('asset_code', models.CharField(max_length=12)),
                ('asset_issuer', models.CharField(max_length=56)),
                ('amount_for_bribes', models.DecimalField(decimal_places=7, max_digits=20, null=True)),
                ('amount_aqua', models.DecimalField(decimal_places=7, max_digits=20, null=True)),
                ('convertation_tx_hash', models.CharField(default=None, max_length=255, null=True)),
                ('refund_tx_hash', models.CharField(default=None, max_length=255, null=True)),
                ('claimable_balance_id', models.CharField(max_length=255, unique=True)),
                ('paging_token', models.CharField(max_length=255)),
                ('unlock_time', models.DateTimeField(null=True)),
                ('start_at', models.DateTimeField(null=True)),
                ('stop_at', models.DateTimeField(null=True)),
                ('created_at', models.DateTimeField()),
                ('loaded_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
        ),
    ]
