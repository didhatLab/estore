import pathlib

from unittest import TestCase

from easystore.substorecreator import SubStoreCreator
from easystore.database import SubStore


class TestSubStoreCreator(TestCase):

    def setUp(self) -> None:
        self.path_new_store = pathlib.Path("new_test.sbstore")
        self.sub_store_creator = SubStoreCreator(pathlib.Path("teststore.estore"))

    def tearDown(self) -> None:
        pathlib.Path("new_test.sbstore.meta").unlink()
        pathlib.Path("new_test.sbstore").unlink()

    def test_create_sub_store_common(self):
        self.sub_store_creator.create_sub_store("new_test", ["name", "id[pk]"])
        sub_store = SubStore(self.path_new_store)
        self.assertEqual(["name", "id"], sub_store.spec)
        res = sub_store.insert_one(name="kek")
        self.assertEqual(1, res)
        new_person = sub_store.get_one()
        self.assertEqual("kek", new_person.name)
        self.assertEqual("1", new_person.id)

