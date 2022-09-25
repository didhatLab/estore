import pathlib

from unittest import TestCase

from easystore.database import EasyStore, SubStore


class TestEasyStore(TestCase):

    def setUp(self) -> None:
        self.easy_store_path = pathlib.Path("easy_test.estore")
        self.easy_store = EasyStore("easy_test.estore")

    def tearDown(self) -> None:
        self.easy_store_path.unlink()
        pathlib.Path("students.sbstore").unlink()
        pathlib.Path("students.sbstore.meta").unlink()

    def test_easy_store_common(self):
        self.easy_store.create_sub_store("students", ["id[pk] name"])
        students_store = self.easy_store.get_sub_store("students")
        test = SubStore(pathlib.Path("students.sbstore"))
        students_store.insert_one(name="kek")
        student = students_store.get_one()
        self.assertEqual("1", student.id)
        self.assertEqual("kek", student.name)
        # print(students_store.insert_one(name="lol"))
        # print(students_store.get_one())
        self.assertEqual(["id", "name"], students_store.spec)

