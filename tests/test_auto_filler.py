from unittest import TestCase

from easystore.autofields import AutoFieldAdder
from easystore.meta import AbstractMetaSubStoreHandler
from easystore.utilis import MetaInfo, Validator, Record


class FakeMetaHandler(AbstractMetaSubStoreHandler):

    def get_meta_data(self) -> MetaInfo:
        return MetaInfo(
            fields=["id", "name"],
            fields_config={"id": [Validator.pk], "name": []},
            pk_sets={"id": {1, 2, 4}}
        )


class TestAutoFieldAdder(TestCase):

    def setUp(self) -> None:
        self.fake_meta = FakeMetaHandler()
        self.auto_filler = AutoFieldAdder(self.fake_meta)

    def tearDown(self) -> None:
        pass

    def test_auto_filler_common(self):
        meta = self.fake_meta.get_meta_data()
        record = Record(meta.fields, name="kek")
        self.assertEqual("kek", record.name)
        with self.assertRaises(AttributeError):
            getattr(record, "id")
        record = self.auto_filler.add_auto_fields(record)
        self.assertEqual(record.id, 5)




