import pathlib
from unittest import TestCase
from pathlib import Path

from easystore.database import Record
from easystore.meta import MetaSubStoreHandler, MetaSubStoreUpdater
from easystore.utilis import Validator
from tests.testutilis import delete_temp_files_v3

TEST_SUB_STORE_META = "test.sbstore.meta"
TEST_SUB_STORE = "test.sbstore"
_SPEC = ["id1[pk]", "id2[pk]", "pok"]
star_id1_set = {1, 3, 4}
start_id2_set = {8, 9}


def create_sub_store_meta_for_complex_test():
    path = Path(TEST_SUB_STORE_META)

    with path.open(mode="w") as stm:
        stm.write(f"{' '.join(_SPEC)}\n#hashes\nid1 1 3 4\nid2 8 9\n#endhashes")
    return path


class TestMetaHandler(TestCase):

    def setUp(self) -> None:
        self.meta_handler = MetaSubStoreHandler(pathlib.Path("testsubstore.sbstore"))

    def test_meta_handler_common(self):
        meta_info = self.meta_handler.get_meta_data()
        right_fields = ["student_id", "name", "surname"]
        pk_hashes = {"student_id": {1, 2, 3, 4}}
        fields_configs = {"student_id": [Validator.pk], "name": [], "surname": []}

        self.assertEqual(right_fields, meta_info.fields)
        self.assertEqual(pk_hashes, meta_info.pk_sets)
        self.assertEqual(fields_configs, meta_info.fields_config)


class TestMetaUpdater(TestCase):

    def setUp(self) -> None:
        self.path_test = create_sub_store_meta_for_complex_test()
        self.path_for_sub_store = pathlib.Path(TEST_SUB_STORE)
        self.meta_handler = MetaSubStoreHandler(pathlib.Path(TEST_SUB_STORE))
        self.meta_updater = MetaSubStoreUpdater(self.path_for_sub_store)
        self.record_1 = Record(_SPEC, id1=7, id2=2, pok="kek")
        self.existing_record = Record(_SPEC, id1=1, id2=8, pok="olo")
        self.start_id1_set = star_id1_set.copy()
        self.start_id2_set = start_id2_set.copy()

    def tearDown(self) -> None:
        delete_temp_files_v3(pathlib.Path(TEST_SUB_STORE_META))

    def test_meta_updater_common(self):
        self.meta_updater.update_meta_pk_hashes(inserted_records=[self.record_1])
        meta_info = self.meta_handler.get_meta_data()
        new_id2_set = self.start_id2_set.copy()
        new_id1_set = self.start_id1_set.copy()
        new_id1_set.add(7)
        new_id2_set.add(2)
        self.assertEqual(new_id1_set, meta_info.pk_sets.get("id1"))
        self.assertEqual(new_id2_set, meta_info.pk_sets.get("id2"))

    def test_meta_updater_delete_pk(self):
        self.meta_updater.update_meta_pk_hashes(deleted_records=[self.existing_record])
        meta_info = self.meta_handler.get_meta_data()
        self.start_id2_set.remove(self.existing_record.id2)
        self.start_id1_set.remove(self.existing_record.id1)
        self.assertEqual(self.start_id1_set, meta_info.pk_sets.get("id1"))
        self.assertEqual(self.start_id2_set, meta_info.pk_sets.get("id2"))

    def test_meta_updater_delete_and_insert(self):
        self.meta_updater.update_meta_pk_hashes(deleted_records=[self.existing_record],
                                                inserted_records=[self.record_1])
        meta_info = self.meta_handler.get_meta_data()
        self.start_id1_set.remove(self.existing_record.id1)
        self.start_id2_set.remove(self.existing_record.id2)
        self.start_id1_set.add(self.record_1.id1)
        self.start_id2_set.add(self.record_1.id2)

        self.assertEqual(self.start_id1_set, meta_info.pk_sets.get("id1"))
        self.assertEqual(self.start_id2_set, meta_info.pk_sets.get("id2"))

