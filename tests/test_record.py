from unittest import TestCase

from easystore.utilis import Record


class TestRecord(TestCase):

    def setUp(self) -> None:
        self.table_spec_students = ["student_id", "name", "surname"]
        self.table_spec_labs = ["name", "subject", "description"]
        self.student_info_1 = (1, "Vadim", "Karpov")

    def test_record_common(self):
        student_id, name, surname = self.student_info_1
        record = Record(self.table_spec_students, student_id, name, surname)

        self.assertEqual(student_id, record.student_id)
        self.assertEqual(name, record.name)
        self.assertEqual(surname, record.surname)
        self.assertEqual(name, record["name"])
        self.assertEqual(surname, record["surname"])
        self.assertEqual(student_id, record["student_id"])

    def test_record_with_kwargs(self):
        student_id, name, surname = self.student_info_1
        student = Record(self.table_spec_students, name=name, student_id=student_id)

        self.assertEqual(name, student.name)
        self.assertEqual(student_id, student.student_id)

    def test_record_with_kwargs_and_args(self):
        student_id, name, surname = self.student_info_1
        student = Record(self.table_spec_students, student_id, name=name, surname=surname)
        self.assertEqual(student_id, student.student_id)
        self.assertEqual(name, student.name)
        self.assertEqual(surname, student.surname)

    def test_record_to_dict(self):
        student_id, name, surname = self.student_info_1
        student = Record(self.table_spec_students, student_id, name, surname)
        student_dict = {"student_id": student_id, "name": name, "surname": surname}
        self.assertEqual(student_dict, dict(student))


