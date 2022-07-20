from populator.management.commands import _cache_data as cache_data
from populator.models import History, ResolvableObjectMigration
from django.test import TestCase, TransactionTestCase
from datetime import date, timedelta
from website.models import ResolvableObject, Dataset
from django.forms.models import model_to_dict
from datetime import date
from website.models import Dataset
from django.db import connection


class SyncDatasetTest(TestCase):
    def test_no_migrated_datasets(self):
        Dataset.objects.create(id='a6c6cead-b5ce-4a4e-8cf5-1542ba708dec', data={})
        cache_data.sync_datasets([])
        self.assertEqual(set(ResolvableObject.objects.all().values_list('deleted_date', flat=True)), set())

    def test_sets_deleted_date_for_datasets_not_in_new_migration(self):
        ids = ['a6c6cead-b5ce-4a4e-8cf5-1542ba708dec', 'd6c6cead-b5ce-4a4e-8cf5-1542ba708ded', 'f6c6cead-b5ce-4a4e-8cf5-1542ba708def']
        Dataset.objects.create(id=ids[0], data={})
        Dataset.objects.create(id=ids[1], data={})
        Dataset.objects.create(id=ids[2], data={})
        cache_data.sync_datasets([ids[1]])
        self.assertEqual(Dataset.objects.get(id=ids[0]).deleted_date, date.today())
        self.assertEqual(Dataset.objects.get(id=ids[1]).deleted_date, None)
        self.assertEqual(Dataset.objects.get(id=ids[2]).deleted_date, date.today())

    def test_does_no_deletions_if_none_deleted(self):
        ids = ['a6c6cead-b5ce-4a4e-8cf5-1542ba708dec', 'd6c6cead-b5ce-4a4e-8cf5-1542ba708ded', 'f6c6cead-b5ce-4a4e-8cf5-1542ba708def']
        for id_ in ids:
            Dataset.objects.create(id=id_, data={})
        cache_data.sync_datasets(ids)
        all_deleted_dates = Dataset.objects.all().values_list('deleted_date', flat=True)
        self.assertEqual(set(all_deleted_dates), {None})
        self.assertEqual(set(ResolvableObject.objects.all().values_list('deleted_date', flat=True)), set())

    def test_deletes_records_in_deleted_datasets(self):
        ids = ['a6c6cead-b5ce-4a4e-8cf5-1542ba708dec', 'd6c6cead-b5ce-4a4e-8cf5-1542ba708ded', 'f6c6cead-b5ce-4a4e-8cf5-1542ba708def']
        for id_ in ids:
            d = Dataset.objects.create(id=id_, data={})
            for i in range(3):
                ResolvableObject.objects.create(id='{}_{}'.format(id_, i), data={}, type='occurrence', dataset=d)
        cache_data.sync_datasets([ids[0]])
        self.assertEqual(set(ResolvableObject.objects.filter(dataset__id=ids[0]).values_list('deleted_date', flat=True)), {None})
        self.assertEqual(set(ResolvableObject.objects.filter(dataset__id=ids[1]).values_list('deleted_date', flat=True)), {date.today()})
        self.assertEqual(set(ResolvableObject.objects.filter(dataset__id=ids[2]).values_list('deleted_date', flat=True)), {date.today()})


class CacheDataTest(TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        self.dataset = Dataset.objects.create(id='dataset_id', data={'title': 'My dataset'})

    def create_ro(self, data={}, id_='a'):  # The old data
        ResolvableObject.objects.create(id=id_, type='occurrence', dataset=self.dataset, data=data)

    def create_ro_migration(self, data={}, id_='a'):  # The newly imported data
        ResolvableObjectMigration.objects.create(id=id_, type='occurrence', dataset_id=self.dataset.id, data=data, parent_id=None)

    def assert_json_equal(self, iterable1, iterable2):  # Necessary as assertEqual does not compare json fields
        self.assertEqual([model_to_dict(x) for x in iterable1], [model_to_dict(x) for x in iterable2])

    def test_create_temp_updated_table(self):
        self.create_ro({'scientificname': 'same', 'location': 'old original'})
        self.create_ro_migration({'scientificname': 'same', 'location': 'new updated'})
        self.create_ro({'scientificname': 'same', 'location': 'same'}, id_='b')
        self.create_ro_migration({'scientificname': 'same', 'location': 'same'}, id_='b')
        cache_data.create_temp_updated_table()
        cache_data.populate_temp_updated_table(2, 0)
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM temp_updated')
            results = cursor.fetchall()
        expected = [('a', '{"location": "old original"}', '{"location": "new updated", "scientificname": "same"}')]
        self.assertEqual(results, expected)

    def test_records_old_version_of_modified_data_items_in_history_table(self):
        self.create_ro({'scientificname': 'same', 'location': 'old original'})
        self.create_ro_migration({'scientificname': 'same', 'location': 'new updated'})
        cache_data.merge_in_new_data()
        expected = History(id=1, resolvable_object_id='a', changed_data={'location': 'old original'}, changed_date=date.today())
        self.assert_json_equal(History.objects.all(), [expected])

    def test_records_multiple_items_in_history_table(self):
        self.create_ro({'scientificname': 'same', 'location': 'old original'})
        self.create_ro_migration({'scientificname': 'same', 'location': 'new updated'})
        self.create_ro({'scientificname': 'same', 'location': 'old original'}, id_='b')
        self.create_ro_migration({'scientificname': 'same', 'location': 'new updated'}, id_='b')
        cache_data.merge_in_new_data()
        expected_a = History(id=1, resolvable_object_id='a', changed_data={'location': 'old original'}, changed_date=date.today())
        expected_b = History(id=2, resolvable_object_id='b', changed_data={'location': 'old original'}, changed_date=date.today())
        self.assert_json_equal(History.objects.all(), [expected_a, expected_b])

    def test_records_copy_of_deleted_data_items_in_history_table(self):
        self.create_ro({'scientificname': 'same', 'incorrect_data': 'to be deleted'})
        self.create_ro_migration({'scientificname': 'same'})
        cache_data.merge_in_new_data()
        expected = History(id=1, resolvable_object_id='a', changed_data={'incorrect_data': 'to be deleted'}, changed_date=date.today())
        self.assert_json_equal(History.objects.all(), [expected])

    def test_adds_none_for_new_created_data_items_history_table(self):
        self.create_ro({'scientificname': 'same'})
        self.create_ro_migration({'scientificname': 'same', 'location': 'new created'})
        cache_data.merge_in_new_data()
        expected = History(id=1, resolvable_object_id='a', changed_data={'location': None}, changed_date=date.today())
        self.assert_json_equal(History.objects.all(), [expected])

    def test_does_not_add_to_history_table_if_no_changes(self):
        self.create_ro({'no_change': 'should not be included in history'})
        self.create_ro_migration({'no_change': 'should not be included in history'})
        cache_data.merge_in_new_data()
        self.assertEqual(list(History.objects.all()), [])

    def test_new_entry_does_not_add_to_history_table(self):
        self.create_ro_migration({'new_record_added_to_dataset': 'should not be included in history'})
        cache_data.merge_in_new_data()
        self.assertEqual(list(History.objects.all()), [])

    def test_updates_changes_from_gbif_in_resolvableobject_table(self):
        self.create_ro({'no_change': 'same', 'updated': 'old original', 'deleted': 'old deleted'})
        self.create_ro_migration({'no_change': 'same', 'updated': 'new updated', 'created': 'new created'})
        cache_data.merge_in_new_data()
        expected = {'no_change': 'same', 'updated': 'new updated', 'created': 'new created'}
        self.assertEqual(ResolvableObject.objects.all()[0].data, expected)

    def test_creates_new_single_record_in_resolvableobject_table(self):
        self.create_ro_migration({'new_record': 'new'})
        cache_data.merge_in_new_data()
        self.assert_json_equal(ResolvableObject.objects.all(), [ResolvableObject(id='a', type='occurrence', data={'new_record': 'new'}, dataset=self.dataset)])

    def test_creates_new_multiple_records_in_resolvable_object_table(self):
        self.create_ro_migration({'new_record': 'new'})
        self.create_ro_migration({'new_record': 'new'}, 'b')
        cache_data.merge_in_new_data()
        expected = [
            ResolvableObject(id='a', data={'new_record': 'new'}, type='occurrence', dataset=self.dataset),
            ResolvableObject(id='b', data={'new_record': 'new'}, type='occurrence', dataset=self.dataset),
        ]
        self.assert_json_equal(ResolvableObject.objects.all(), expected)

    def test_adds_deleted_datestamp_for_removed_records_in_resolvable_object_table(self):
        self.create_ro({'no_change': 'same'})
        cache_data.merge_in_new_data()
        expected = ResolvableObject(id='a', data={'no_change': 'same'}, deleted_date=date.today(), type='occurrence', dataset=self.dataset)
        self.assert_json_equal(ResolvableObject.objects.all(), [expected])

    def test_does_not_overwrite_preexisting_deleted_datestamps(self):
        past_date_d = date.today() - timedelta(days=4)
        ResolvableObject.objects.create(id='1', data={'none': 'none'}, deleted_date=past_date_d, dataset=self.dataset, type='occurrence')
        cache_data.merge_in_new_data()
        self.assertEqual(ResolvableObject.objects.first().deleted_date, past_date_d)

    def test_record_is_deleted_and_then_gets_added_again(self):
        # I have no idea what to do here. For the moment we can assume it's a blip and remove the deleted date again?
        self.create_ro({'key': 'value'})
        cache_data.merge_in_new_data()
        self.assertEqual(ResolvableObject.objects.first().deleted_date, date.today())
        self.create_ro_migration({'key': 'value2'})
        cache_data.merge_in_new_data()
        self.assertEqual(ResolvableObject.objects.count(), 1)
        self.assertEqual(ResolvableObject.objects.first().deleted_date, None)

