import abc

from typing import List

from easystore.meta import AbstractMetaSubStoreHandler
from easystore.utilis import MetaInfo, Record, Validator


class AbstractAutoFieldsAdder(metaclass=abc.ABCMeta):
    AUTO_FIELDS_CONFIGS = {Validator.pk}

    @abc.abstractmethod
    def add_auto_fields(self, spec):
        raise NotImplementedError()


class AutoFieldAdder(AbstractAutoFieldsAdder):

    def __init__(self, meta_handler: AbstractMetaSubStoreHandler):
        self.meta_handler = meta_handler

    def add_auto_fields(self, record: Record) -> Record:
        return self._add_auto_fields(record)

    def _add_auto_fields(self, record: Record) -> Record:
        meta_info = self.meta_handler.get_meta_data()
        params_with_auto_filling = self.__get_params_with_auto_filling(meta_info)
        return self.__add_auto_fields(meta_info, params_with_auto_filling, record)

    @staticmethod
    def __add_auto_fields(meta_info: MetaInfo, params_with_auto_filling: List[str],
                          record: Record) -> Record:
        for auto_param in params_with_auto_filling:
            if not getattr(record, auto_param, None):
                params_pk_set = meta_info.pk_sets.get(auto_param)
                auto_param_value = max(params_pk_set) + 1 if params_pk_set else 1
                setattr(record, auto_param, auto_param_value)
        return record

    def __get_params_with_auto_filling(self, meta_info: MetaInfo) -> List[str]:
        params_with_auto_adding = []
        for param_name, param_configs in meta_info.fields_config.items():
            for config in param_configs:
                if config in self.AUTO_FIELDS_CONFIGS:
                    params_with_auto_adding.append(param_name)
        return params_with_auto_adding
