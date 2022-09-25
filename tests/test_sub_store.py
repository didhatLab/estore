import pathlib
from pathlib import Path
from unittest import TestCase

from easystore.database import SubStore
from easystore.errors import NotFoundField, UniqueKeyError
from tests.testutilis import delete_temp_files_v3

PATH_TEST_INSERT_FILE = "insert_test.sbstore"
PATH_TEST_DELETE_FILE = "delete_test.sbstore"


def create_test_sub_store_for_inserting():
    path = Path(PATH_TEST_INSERT_FILE)
    with path.open('w') as st:
        st.write("#records")
    meta_path = Path(f"{PATH_TEST_INSERT_FILE}.meta")
    with meta_path.open("w") as stm:
        stm.write("id name kotlin\n#hashes\n#endhashes")

    return path


def create_test_sub_store_for_deleting():
    path = Path(PATH_TEST_DELETE_FILE)
    with path.open(mode="w") as st:
        st.write("#records\n")
        st.write("1 dan\n")
        st.write("2 kek\n")
        st.write("3 lol\n")
    meta_path = Path(f"{PATH_TEST_DELETE_FILE}.meta")
    with meta_path.open(mode="w") as stm:
        stm.write("id[pk] name\n#hashes\nid 1 2 3\n#endhashes")
    return path


def delete_temp_files_v2(*files: pathlib.Path):
    for file in files:
        meta_file = file.parent / pathlib.Path(f"{file.name}.meta")
        meta_file.unlink()
        file.unlink()


def delete_temp_files():
    path = Path(PATH_TEST_INSERT_FILE)
    meta_path = Path(f"{PATH_TEST_INSERT_FILE}.meta")
    path.unlink()
    meta_path.unlink()


class TestSubStore(TestCase):

    def setUp(self) -> None:
        self.sub_store_path = Path("testsubstore.sbstore")
        self.sub_store = SubStore(self.sub_store_path)
        self.insert = create_test_sub_store_for_inserting()
        self.sub_store_for_inserting = SubStore(self.insert)

    def tearDown(self) -> None:
        delete_temp_files()

    def test_sub_store_spec(self):
        sub_store = SubStore(self.sub_store_path)
        right_spec = ["student_id", "name", "surname"]
        self.assertEqual(right_spec, sub_store.spec)

    def test_get_one_without_conditions(self):
        sub_store = SubStore(self.sub_store_path)
        record = sub_store.get_one()
        self.assertEqual("1", record.student_id)
        self.assertEqual("dan", record.name)
        self.assertEqual("kolo", record.surname)

    def test_get_one_with_one_condition(self):
        sub_store = SubStore(self.sub_store_path)
        record = sub_store.get_one(name="didhat")
        self.assertEqual("2", record.student_id)
        self.assertEqual("didhat", record.name)
        self.assertEqual("jojo", record.surname)

    def test_get_one_with_many_conditions(self):
        sub_store = SubStore(self.sub_store_path)
        record = sub_store.get_one(name="kadim", surname="varpov")
        self.assertEqual("3", record.student_id)
        self.assertEqual("kadim", record.name)
        self.assertEqual("varpov", record.surname)

    def test_get_one_without_matches(self):
        record = self.sub_store.get_one(student_id=31413121)
        self.assertEqual(record, None)

    def test_get_one_with_non_right_field(self):
        with self.assertRaises(NotFoundField) as context:
            self.sub_store.get_one(non_exist_field="test")

    def test_get_many_without_conditions(self):
        students = self.sub_store.get_many()
        student_dan = students[0]
        self.assertEqual(student_dan.name, "dan")
        self.assertEqual(student_dan.student_id, "1")
        self.assertEqual(student_dan.surname, "kolo")
        self.assertEqual(len(students), 4)
        student_kadim = students[2]
        self.assertEqual(student_kadim.name, "kadim")
        self.assertEqual(student_kadim.surname, "varpov")
        self.assertEqual(student_kadim.student_id, "3")

    def test_get_many_with_condition(self):
        students = self.sub_store.get_many(name="kadim")
        self.assertEqual(2, len(students))
        student_1 = students[0]

        self.assertEqual(student_1.name, "kadim")
        self.assertEqual(student_1.surname, "varpov")
        self.assertEqual(student_1.student_id, "3")

        student_2 = students[1]

        self.assertEqual(student_2.name, "kadim")
        self.assertEqual(student_2.surname, "karpov")
        self.assertEqual(student_2.student_id, "4")

    def test_get_many_with_many_conditions(self):
        students = self.sub_store.get_many(name="didhat", surname="jojo")
        self.assertEqual(len(students), 1)
        student_didhat = students[0]
        self.assertEqual("didhat", student_didhat.name)
        self.assertEqual("jojo", student_didhat.surname)
        self.assertEqual("2", student_didhat.student_id)

    def test_get_many_without_matches(self):
        students = self.sub_store.get_many(name="dododo")
        self.assertEqual(0, len(students))

    def test_get_many_with_error_field(self):
        with self.assertRaises(NotFoundField):
            self.sub_store.get_many(non_exist_field="test")

    def test_get_all(self):
        students = self.sub_store.get_all()
        self.assertEqual(4, len(students))

    def test_insert_one_common(self):
        res = self.sub_store_for_inserting.insert_one("1", "dan", "ok")
        self.assertEqual(1, res)
        student = self.sub_store_for_inserting.get_one()
        self.assertEqual("1", student.id)
        self.assertEqual("dan", student.name)
        self.assertEqual("ok", student.kotlin)

    def test_insert_one_with_kwargs(self):
        res = self.sub_store_for_inserting.insert_one(id="2", name="vadim", kotlin="no")
        self.assertEqual(1, res)
        student = self.sub_store_for_inserting.get_one(id="2")
        self.assertEqual("2", student.id)
        self.assertEqual("vadim", student.name)
        self.assertEqual("no", student.kotlin)

    def test_insert_one_with_args_and_with_kwargs(self):
        res = self.sub_store_for_inserting.insert_one("3", "kate", kotlin="yes")
        self.assertEqual(1, res)
        student = self.sub_store_for_inserting.get_one(id="3")
        self.assertEqual("3", student.id)
        self.assertEqual("kate", student.name)
        self.assertEqual("yes", student.kotlin)

    def test_insert_one_with_args_and_with_kwargs_2(self):
        res = self.sub_store_for_inserting.insert_one("4", "vasya", "yes", name="vanya")
        self.assertEqual(1, res)
        student = self.sub_store_for_inserting.get_one(id="4")
        self.assertEqual("4", student.id)
        self.assertEqual("vanya", student.name)
        self.assertEqual("yes", student.kotlin)

    def test_insert_without_some_params(self):
        res = self.sub_store_for_inserting.insert_one(name="moro")
        self.assertEqual(1, res)
        student = self.sub_store_for_inserting.get_one()
        self.assertEqual("moro", student.name)
        self.assertEqual("None", student.id)
        self.assertEqual("None", student.kotlin)


class TestSubStoreDeleting(TestCase):

    def setUp(self) -> None:
        self.test_path = create_test_sub_store_for_deleting()
        self.sub_store = SubStore(self.test_path)

    def tearDown(self) -> None:
        delete_temp_files_v2(self.test_path)

    def test_delete_one_common(self):
        person = self.sub_store.get_one(name="dan")
        self.assertEqual("dan", person.name)
        self.assertEqual("1", person.id)
        res = self.sub_store.delete_one()
        self.assertEqual(1, res)
        person_none = self.sub_store.get_one(name="dan")
        self.assertEqual(person_none, None)

    def test_delete_one_with_conditions(self):
        person = self.sub_store.get_one(id=2)
        self.assertEqual("kek", person.name)
        self.assertEqual("2", person.id)
        res = self.sub_store.delete_one(id=2)
        self.assertEqual(1, res)
        person_none = self.sub_store.get_one(id=2)
        self.assertEqual(None, person_none)

    def test_delete_one_several_times(self):
        res = self.sub_store.delete_one(id=1)
        self.assertEqual(1, res)
        res = self.sub_store.delete_one(id=2)
        self.assertEqual(1, res)
        res = self.sub_store.delete_one(id=2)
        self.assertEqual(0, res)


def create_sub_store_with_meta(path: str):
    sub_store_path = pathlib.Path(path)
    sub_store_meta_path = pathlib.Path(f"{path}.meta")

    with sub_store_path.open(mode="w") as st:
        st.write("#records\n1 dan\n2 max\n3 vadim")
    with sub_store_meta_path.open(mode="w") as stm:
        stm.write("id[pk] name\n#hashes\nid 1 2 3\n#endhashes")
    return sub_store_path


class TestInsertDeleteOperationsWithMeta(TestCase):

    def setUp(self) -> None:
        self.test_str_path = "test_insert.sbstore"
        self.test_path = create_sub_store_with_meta(self.test_str_path)
        self.sub_store = SubStore(self.test_path)

    def tearDown(self) -> None:
        delete_temp_files_v3(self.test_path, pathlib.Path(f"{self.test_str_path}.meta"))

    def test_common_insert(self):
        student_dan = self.sub_store.get_one(id=1)
        self.assertEqual("1", student_dan.id)
        self.assertEqual("dan", student_dan.name)

        res = self.sub_store.delete_one(id=1)
        self.assertEqual(1, res)
        res = self.sub_store.insert_one(id=1, name="Kot")
        self.assertEqual(1, res)
        new_student = self.sub_store.get_one(id=1)
        self.assertEqual("Kot", new_student.name)

    def test_insert_one_without_id(self):
        res = self.sub_store.insert_one(name="jojo")
        self.assertEqual(1, res)
        inserted_person = self.sub_store.get_one(id=4)
        self.assertEqual("jojo", inserted_person.name)
        self.assertEqual("4", inserted_person.id)

    def test_insert_with_unique_error(self):
        with self.assertRaises(UniqueKeyError):
            self.sub_store.insert_one(id=1, name="jojo")

    def test_insert_many(self):
        pass

    def test_insert_many_with_error_in_one(self):
        pass

    def test_delete_one(self):
        res = self.sub_store.delete_one(id=1)
        self.assertEqual(1, res)
        res = self.sub_store.insert_one(id=1, name="kek")
        self.assertEqual(1, res)
