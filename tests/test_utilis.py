from unittest import TestCase

from easystore.utilis import Record, params2record


def test_record_handler(obj, record: Record):
    return record


class TestUtilis(TestCase):

    def setUp(self) -> None:
        self.obj = object()

    def tearDown(self) -> None:
        pass

    def test_decorator_params2record(self):

        dec_func = params2record(test_record_handler)
        res = dec_func(self.obj, ["name"], name="Dan")
        self.assertTrue(isinstance(res, Record))
        self.assertEqual("Dan", res.name)

    def test_decorator_params2record_with_args(self):
        dec_func = params2record(test_record_handler)
        res = dec_func(self.obj, ["name", "id"], "Kek", id=1)
        self.assertEqual("Kek", res.name)
        self.assertEqual(1, res.id)
