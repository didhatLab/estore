from unittest import TestCase

from easystore.specparser import SpecParser


class TestSpecParser(TestCase):

    def setUp(self) -> None:
        self.spec_parser = SpecParser()

    def tearDown(self) -> None:
        pass

    def test_spec_parser_common(self):
        list_spec = ["id[pk]", "name"]
        spec_info = self.spec_parser.parse_spec_list(list_spec)
        self.assertEqual(["id", "name"], spec_info.fields)
        self.assertEqual({"id": ["pk"], "name": []}, spec_info.fields_configs)

    def test_spec_parser_string_spec(self):
        spec = "id[pk] name"
        spec_info = self.spec_parser.parse_spec_string(spec)
        self.assertEqual(["id", "name"], spec_info.fields)
        self.assertEqual({"id": ["pk"], "name": []}, spec_info.fields_configs)
