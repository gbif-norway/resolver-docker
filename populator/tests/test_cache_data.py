from populator.management.commands import _cache_data as cache_data
from populator.models import History, ResolvableObjectMigration
from django.test import TestCase, TransactionTestCase
from datetime import date, timedelta
from website.models import ResolvableObject, Dataset
from django.forms.models import model_to_dict


class CacheDataTest(TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        self.dataset = Dataset.objects.create(id='dataset_id', data={'title': 'My dataset'})

    def create_ro(self, data={}):
        ResolvableObject.objects.create(id='a', type='occurrence', dataset=self.dataset, data=data)

    def create_ro_migration(self, data={}, id='a'):
        ResolvableObjectMigration.objects.create(id=id, type='occurrence', dataset_id=self.dataset.id, data=data)

    def assert_equal(self, iterable1, iterable2):  # Necessary as assertEqual does not compare json fields
        self.assertEqual([model_to_dict(x) for x in iterable1], [model_to_dict(x) for x in iterable2])

    def test_records_old_version_of_modified_data_items_in_history_table(self):
        self.create_ro({'scientificname': 'same', 'location': 'old original'})
        self.create_ro_migration({'scientificname': 'same', 'location': 'new updated'})
        cache_data.merge_in_new_data()
        expected = History(id=1, resolvable_object_id='a', changed_data={'location': 'old original'}, changed_date=date.today())
        self.assert_equal(History.objects.all(), [expected])

    def test_records_copy_of_deleted_data_items_in_history_table(self):
        self.create_ro({'scientificname': 'same', 'incorrect_data': 'to be deleted'})
        self.create_ro_migration({'scientificname': 'same'})
        cache_data.merge_in_new_data()
        expected = History(id=1, resolvable_object_id='a', changed_data={'incorrect_data': 'to be deleted'}, changed_date=date.today())
        self.assert_equal(History.objects.all(), [expected])

    def test_adds_none_for_new_created_data_items_history_table(self):
        self.create_ro({'scientificname': 'same'})
        self.create_ro_migration({'scientificname': 'same', 'location': 'new created'})
        cache_data.merge_in_new_data()
        expected = History(id=1, resolvable_object_id='a', changed_data={'location': None}, changed_date=date.today())
        self.assert_equal(History.objects.all(), [expected])

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
        self.assert_equal(ResolvableObject.objects.all(), [ResolvableObject(id='a', type='occurrence', data={'new_record': 'new'}, dataset=self.dataset)])

    def test_creates_new_multiple_records_in_resolvable_object_table(self):
        self.create_ro_migration({'new_record': 'new'})
        self.create_ro_migration({'new_record': 'new'}, 'b')
        cache_data.merge_in_new_data()
        expected = [
            ResolvableObject(id='a', data={'new_record': 'new'}, type='occurrence', dataset=self.dataset),
            ResolvableObject(id='b', data={'new_record': 'new'}, type='occurrence', dataset=self.dataset),
        ]
        self.assert_equal(ResolvableObject.objects.all(), expected)

    def test_adds_deleted_datestamp_for_removed_records_in_resolvable_object_table(self):
        self.create_ro({'no_change': 'same'})
        cache_data.merge_in_new_data()
        expected = ResolvableObject(id='a', data={'no_change': 'same'}, deleted_date=date.today(), type='occurrence', dataset=self.dataset)
        self.assert_equal(ResolvableObject.objects.all(), [expected])

    def test_does_not_overwrite_preexisting_deleted_datestamps(self):
        past_date_d = date.today() - timedelta(days=4)
        ResolvableObject.objects.create(id='1', data={'none': 'none'}, deleted_date=past_date_d, dataset=self.dataset, type='occurrence')
        cache_data.merge_in_new_data()
        self.assertEqual(ResolvableObject.objects.first().deleted_date, past_date_d)
