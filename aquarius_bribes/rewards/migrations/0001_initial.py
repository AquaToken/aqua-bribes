# Generated by Django 3.2.12 on 2022-02-18 07:44

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('bribes', '0002_auto_20220217_1619'),
    ]

    operations = [
        migrations.CreateModel(
            name='VoteSnapshot',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('market_key', models.CharField(max_length=56)),
                ('votes_value', models.DecimalField(decimal_places=7, max_digits=20)),
                ('voting_account', models.CharField(max_length=56)),
                ('snapshot_time', models.DateTimeField()),
            ],
        ),
        migrations.CreateModel(
            name='Payout',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('stellar_transaction_id', models.CharField(blank=True, max_length=64)),
                ('status', models.CharField(choices=[('success', 'success'), ('failed', 'failed')], default='success', max_length=30)),
                ('message', models.TextField(blank=True)),
                ('reward_amount', models.DecimalField(decimal_places=7, max_digits=20, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('bribe', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='bribes.bribe')),
                ('vote_snapshot', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='rewards.votesnapshot')),
            ],
        ),
    ]