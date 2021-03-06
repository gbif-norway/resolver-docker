# Generated by Django 3.1 on 2020-08-21 10:25

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('website', '0001_initial'),
        ('populator', 'jsonb_diff_val'),
    ]

    operations = [
        migrations.CreateModel(
            name='ResolvableObjectMigration',
            fields=[
                ('id', models.CharField(max_length=200, primary_key=True, serialize=False)),
                ('data', models.JSONField()),
                ('type', models.CharField(max_length=200)),
                ('dataset_id', models.CharField(max_length=200)),
            ],
        ),
        migrations.CreateModel(
            name='Statistic',
            fields=[
                ('name', models.CharField(max_length=100, primary_key=True, serialize=False)),
                ('value', models.IntegerField()),
            ],
        ),
        migrations.CreateModel(
            name='History',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('changed_data', models.JSONField()),
                ('changed_date', models.DateField()),
                ('resolvable_object', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='website.resolvableobject')),
            ],
        ),
    ]
