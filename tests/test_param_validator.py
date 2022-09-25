from unittest import TestCase

from easystore.utilis import MetaInfo, Validator
from easystore.fieldsvalidator import ParamForFieldValidator
from easystore.errors import UniqueKeyError, NegativePrimaryKeyError


class TestParamValidator(TestCase):

    def setUp(self) -> None:
        self.pk_id_name = "test_id"
        self.pk_id_validators = [Validator.pk, ]
        self.meta_data = MetaInfo(["dan", "kek", self.pk_id_name],
                                  {"dan": [], "kek": [], self.pk_id_name: self.pk_id_validators},
                                  {self.pk_id_name: {1, 2, 3, 4, 6}}, )
        self.validator = ParamForFieldValidator(meta_data_from_sub_store=self.meta_data)

    def test_validate_pk_field_common(self):
        new_pk_id = 10
        res = self.validator.validate(self.pk_id_name, new_pk_id)
        self.assertEqual(True, res)

    def test_validate_pk_field_with_error(self):
        new_pk_id = 1
        with self.assertRaises(UniqueKeyError):
            self.validator.validate(self.pk_id_name, new_pk_id)

    def test_validate_negative_pk_error(self):
        error_pk_id = -1
        with self.assertRaises(NegativePrimaryKeyError):
            self.validator.validate(self.pk_id_name, error_pk_id)

    def test_validate_type_error(self):
        error_pk_id = "3121"
        with self.assertRaises(TypeError):
            self.validator.validate(self.pk_id_name, error_pk_id)

