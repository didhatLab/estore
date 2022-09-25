import abc
import enum

from typing import List, Callable, TypeVar

from easystore.utilis import MetaInfo, Validator
from easystore import errors

FIELDS_CONFIGS = {"pk": {"unique": True, "type": [int, str]}}


class AbstractForFieldValidator(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def validate(self, param_name, value):
        raise NotImplementedError()


class ParamForFieldValidator(AbstractForFieldValidator):

    # TODO: redo this shit code

    def __init__(self, meta_data_from_sub_store: MetaInfo):
        self.__fields = meta_data_from_sub_store.fields
        self.__fields_configs = meta_data_from_sub_store.fields_config
        self.__pk_sets = meta_data_from_sub_store.pk_sets
        self.__sub_store_name = "test"

    def validate(self, param_name, value, inserting=True):
        configs = self.__fields_configs.get(param_name, [])
        param_validators = self.__get_right_validators(configs)
        self._validate(param_name, value, param_validators, inserting)
        return True

    def check_fields(self, inserting=False, **conditions):
        self._check_fields(inserting=inserting, **conditions)
        return True

    def _check_fields(self, inserting=False, **conditions):
        for name, value in conditions.items():
            if name not in self.__fields:
                raise errors.NotFoundField()
            self.validate(name, value, inserting=inserting)

    @staticmethod
    def _validate(param_name, value, param_validators: List[Callable], inserting=True):
        for validator in param_validators:
            validator(param_name, value, inserting)
        return

    def __get_right_validators(self, configs: List[Validator]) -> List[Callable]:
        record_validators = []
        for config in configs:
            param_validator = self.__get_right_validator(config)
            record_validators.append(param_validator)
        return record_validators

    def __get_right_validator(self, config: Validator) -> Callable:

        if config == Validator.pk:
            return self._pk_validator

    def _pk_validator(self, param_name: str, value: int, inserting=True):
        if not isinstance(value, int):
            raise TypeError("{} is pk field, must be positive integer".format(param_name))

        if value <= 0:
            raise errors.NegativePrimaryKeyError("{} is pk field, must be positive integer".format(param_name))
        if inserting:
            self.__pk_validator(param_name, value)

    def __pk_validator(self, param_name: str, value: int):
        hash_set_of_exists_keys = self.__pk_sets.get(param_name, {})
        if value in hash_set_of_exists_keys:
            raise errors.UniqueKeyError("Key {} exists in SubStore {}".format(value, self.__sub_store_name))
        return
