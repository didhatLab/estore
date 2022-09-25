import abc
import pathlib

from dataclasses import dataclass
from typing import List, Dict, Set, Optional

from easystore.utilis import (config2validator_type,
                              MetaInfo,
                              go_to_store_point,
                              read_data_until_point,
                              FilePoints,
                              Record, temp_file2main)


@dataclass(frozen=True)
class MetaTypes:
    START_KEYS_HASHES = "#hashes"
    END_START_HASHES = "#endhashes"


class AbstractMetaSubStoreHandler(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def get_meta_data(self) -> MetaInfo:
        raise NotImplementedError()


class AbstractMetaSubStoreUpdater(metaclass=abc.ABCMeta):
    pass
    # @abc.abstractmethod
    # def update_meta_pk_hashes(self, pk_name: str, pk_key: int):
    #     pass


class MetaSubStoreHandler(AbstractMetaSubStoreHandler):

    def __init__(self, main_sub_store_path: pathlib.Path):
        self.meta_file_path = main_sub_store_path.parent / pathlib.Path(f"{main_sub_store_path.name}.meta")

    def get_meta_data(self) -> MetaInfo:
        return self._get_meta_data()

    def _get_meta_data(self):
        spec, spec_config, keys_hashes = self.__get_meta_data()
        return MetaInfo(fields=spec, fields_config=spec_config, pk_sets=keys_hashes)

    def __get_meta_data(self):
        spec, spec_configs = self.__get_spec_data()
        keys_hashes = self.__get_hashes_for_keys()
        return spec, spec_configs, keys_hashes

    def __get_spec_data(self):
        with self.meta_file_path.open() as stm:
            spec_line = stm.readline()
            spec_configs = {}
            spec = []
            for field in spec_line.split():
                field_name, *field_configs = field.replace(']', '').replace('[', ' ').split(' ')
                spec.append(field_name)
                spec_configs[field_name] = [config2validator_type(config) for config in field_configs]

        return spec, spec_configs

    def __get_hashes_for_keys(self):
        keys_hashes = {}
        with self.meta_file_path.open() as stm:
            go_to_store_point(stm, FilePoints.PK_SETS_START)

            for line in read_data_until_point(stm, FilePoints.PK_SETS_END):
                key, *hash_keys = line.split()
                keys_hashes[key] = {int(key) for key in hash_keys}

        return keys_hashes


class MetaSubStoreUpdater(AbstractMetaSubStoreUpdater):

    def __init__(self, sub_store_path: pathlib.Path):
        self.__meta_sub_store_path = pathlib.Path(f"{str(sub_store_path)}.meta")
        self.__meta_handler = MetaSubStoreHandler(sub_store_path)

    def update_meta_pk_hashes(self,
                              deleted_records: List[Record] = None,
                              inserted_records: List[Record] = None):
        deleted_records = deleted_records if deleted_records else []
        inserted_records = inserted_records if inserted_records else []

        self._update_meta_pk_hashes(deleted_records, inserted_records)

    def _update_meta_pk_hashes(self, deleted_records: List[Record],
                               inserted_records: List[Record]):
        meta_info = self.__meta_handler.get_meta_data()
        primary_keys = meta_info.pk_sets.keys()
        key_values_for_deleting: Dict[str, Set] = {key: set() for key in primary_keys}
        key_values_for_inserting: Dict[str, List] = {key: [] for key in primary_keys}
        for record in deleted_records:
            for key_name in primary_keys:
                key_value = getattr(record, key_name)
                if key_value:
                    key_values_for_deleting[key_name].add(key_value)
        for record in inserted_records:
            for key_name in primary_keys:
                key_value = getattr(record, key_name)
                if key_value:
                    key_values_for_inserting[key_name].append(key_value)
        self.__update_meta_pk_hashes(key_values_for_deleting, key_values_for_inserting)

    def __update_meta_pk_hashes(self, key_values_for_deleting: Dict[str, Set],
                                key_values_for_inserting: Dict[str, List]):
        temp_meta = pathlib.Path(f"{self.__meta_sub_store_path.parent}/"
                                 f"{self.__meta_sub_store_path.name}.temp")
        with self.__meta_sub_store_path.open(mode="r") as stm:
            with temp_meta.open(mode="w") as stm_temp:
                for line in read_data_until_point(stm, FilePoints.PK_SETS_START):
                    stm_temp.write(line)
                stm_temp.write(f"{FilePoints.PK_SETS_START}\n")
                for line in read_data_until_point(stm, FilePoints.PK_SETS_END):
                    pk_name, *pk_list = line.split()
                    pk_set = set(pk_list)
                    for pk_for_delete in key_values_for_deleting.get(pk_name):
                        pk_set.remove(str(pk_for_delete))
                    for pk_for_adding in key_values_for_inserting.get(pk_name):
                        pk_set.add(pk_for_adding)
                    query = f"{pk_name} {' '.join([str(pk) for pk in pk_set])}\n"
                    stm_temp.write(query)
                stm_temp.write(f"{FilePoints.PK_SETS_END}\n")
        self.__meta_sub_store_path = temp_file2main(self.__meta_sub_store_path, temp_meta)

    def _add_new_pk_key(self, pk_name: str, pk_key: int):
        pass
