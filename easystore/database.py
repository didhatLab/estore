import abc
import os
import pathlib

from typing import Optional, List, Any, Generator
from dataclasses import dataclass
from itertools import zip_longest

from easystore.substorecreator import SubStoreCreator
from easystore.utilis import go_to_store_point, read_data_until_point, FilePoints
from easystore.fieldsvalidator import ParamForFieldValidator
from easystore.autofields import AutoFieldAdder
from easystore.meta import MetaSubStoreHandler, MetaSubStoreUpdater
from easystore.utilis import Record, AbstractRecord, temp_file2main, params2record


@dataclass(frozen=True)
class SubStorePoints:
    RECORDS_START = "#records"


class AbstractEasyStore(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def execute(self, query: Optional[str]):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_sub_store(self, table_name: str) -> "AbstractSubStore":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_sub_store(self, name: str, spec: List[str]):
        raise NotImplementedError()


class AbstractSubStore(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def get_all(self) -> List["AbstractRecord"]:
        return self.get_many()

    @abc.abstractmethod
    def get_many(self, **conditions) -> List["AbstractRecord"]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_one(self, **conditions) -> "AbstractRecord":
        raise NotImplementedError()

    @abc.abstractmethod
    def insert_one(self, *args, **kwargs):
        raise NotImplementedError


class EasyStore(AbstractEasyStore):

    def __init__(self, db_path):
        self.__path = pathlib.Path(db_path)
        self.__init_store(self.__path)
        self.__sub_store_creator = SubStoreCreator(self.__path)

    def execute(self, query: Optional[str]):
        raise NotImplementedError()

    def get_sub_store(self, sub_store_name: str) -> "SubStore":
        return self._get_sub_store(sub_store_name)

    def get_sub_store_list(self) -> List[str]:
        return self._get_list_stores()

    def create_sub_store(self, name: str, spec: List[str]):
        self._create_sub_store(name, spec)

    def _create_sub_store(self, name: str, spec: List[str]):
        self.__sub_store_creator.create_sub_store(name, spec)
        self._add_new_sub_store_to_info(name)

    def _get_sub_store(self, name: str) -> "SubStore":
        sub_store_list = self._get_list_stores()
        if name not in sub_store_list:
            raise FileNotFoundError()
        sub_store_path = self.__path.parent / pathlib.Path(f"{name}.sbstore")
        return SubStore(sub_store_path)

    def _add_new_sub_store_to_info(self, name: str):
        with self.__path.open(mode="a") as es:
            es.write(f"{name}\n")

    def _get_list_stores(self) -> List[str]:
        sub_stores: List[str] = []
        with self.__path.open(mode="r") as es:
            go_to_store_point(es, FilePoints.SUB_STORES_START)
            for sub_store_name in read_data_until_point(es, FilePoints.END):
                sub_stores.append(sub_store_name.replace("\n", ""))
        return sub_stores

    @staticmethod
    def __init_store(path: pathlib.Path):
        if path.exists():
            return
        else:
            path.write_text("EasyStore - Basic file storage\n#substores\n")


class SubStore(AbstractSubStore):

    def __init__(self, sub_store_path: pathlib.Path):
        self.__sub_store_path = sub_store_path
        self.__meta_collector = MetaSubStoreHandler(sub_store_path)
        self.__sub_store_meta_updater = MetaSubStoreUpdater(sub_store_path)

    @property
    def spec(self) -> List[str]:
        return self.__meta_collector.get_meta_data().fields

    def __check_fields(self, inserting=False, **conditions):
        sub_store_validator = ParamForFieldValidator(self.__meta_collector.get_meta_data())

        return sub_store_validator.check_fields(inserting=inserting, **conditions)

    def insert_one(self, *args, **kwargs) -> int:
        return self._insert_one(*args, **kwargs)

    def delete_one(self, **conditions) -> int:
        return self._delete_one(**conditions)

    def get_all(self) -> List[Record]:
        return self.get_many()

    def get_one(self, **conditions) -> Optional[Record]:
        return self._get_one(**conditions)

    def get_many(self, **conditions) -> List[Record]:
        return self._get_many(**conditions)

    def lazy_load(self, **conditions) -> Generator[Record, None, None]:
        return self._lazy_load(**conditions)

    def _insert_one(self, *args, **kwargs) -> int:
        self.__check_fields(inserting=True, **kwargs)
        return self.__insert_one(self.spec, *args, **kwargs)

    def _delete_one(self, **conditions) -> int:
        self.__check_fields(**conditions)
        return self.__delete_one(**conditions)

    def _get_one(self, **conditions) -> Optional[Record]:
        self.__check_fields(**conditions)
        return self.__get_one(**conditions)

    def _get_many(self, **conditions) -> List[Record]:
        self.__check_fields(**conditions)
        return self.__get_many(**conditions)

    def _lazy_load(self, **conditions):
        self.__check_fields(**conditions)
        return self.__lazy_load(**conditions)

    @params2record
    def __insert_one(self, record: Record) -> int:
        auto_filler = AutoFieldAdder(self.__meta_collector)
        record = auto_filler.add_auto_fields(record)
        inserted_rows = 0
        params = {}
        for field in self.spec:
            params[field] = record[field]

        query = " ".join([str(value) for value in params.values()])

        with self.__sub_store_path.open(encoding='utf-8', mode='a') as st:
            st.write("\n{}".format(query))
            inserted_rows += 1
        inserted_record = Record(self.spec, **params)
        self.__sub_store_meta_updater.update_meta_pk_hashes(inserted_records=[inserted_record])
        return inserted_rows

    def __delete_one(self, **conditions) -> int:
        result = 0
        deleted_records: List[Record] = []
        temp_file = self.__sub_store_path.parent / pathlib.Path(f"{self.__sub_store_path.name}.temp")
        with self.__sub_store_path.open(mode="r") as st:
            with temp_file.open(mode="w") as temp_st:
                for line in read_data_until_point(st, FilePoints.RECORDS_START):
                    temp_st.write(line)
                temp_st.write(f"{FilePoints.RECORDS_START}\n")
                for string_data in read_data_until_point(st, FilePoints.RECORDS_END):
                    data_list = [field for field in string_data.split() if field]
                    record = Record(self.spec, *data_list)
                    if not self.__check_record_to_condition(record, **conditions) or result == 1:
                        temp_st.write(f"{string_data}")
                    else:
                        deleted_records.append(record)
                        result += 1

        self.__sub_store_path = temp_file2main(self.__sub_store_path, temp_file)
        self.__sub_store_meta_updater.update_meta_pk_hashes(deleted_records=deleted_records)
        return result

    def __get_one(self, **conditions) -> Optional[Record]:
        result = None
        with self.__sub_store_path.open(encoding='utf-8') as st:

            go_to_store_point(st, FilePoints.RECORDS_START)
            string_data = st.readline()
            while string_data and result is None:
                data_list = [field for field in string_data.split() if field]
                data_record = Record(self.spec, *data_list)
                if self.__check_record_to_condition(data_record, **conditions):
                    result = data_record
                string_data = st.readline()
        return result

    def __get_many(self, **conditions) -> List[Record]:
        result = []
        with self.__sub_store_path.open(encoding='utf-8') as st:
            go_to_store_point(st, FilePoints.RECORDS_START)
            string_data = st.readline()
            while string_data:
                data_list = [field for field in string_data.split() if field]
                data_record = Record(self.spec, *data_list)
                if self.__check_record_to_condition(data_record, **conditions):
                    result.append(data_record)
                string_data = st.readline()
        return result

    def __lazy_load(self, **conditions):
        record_gen = self.__generator_records_from_store()
        record = next(record_gen)
        while record:
            if self.__check_record_to_condition(record, **conditions):
                yield record
        return

    def __generator_records_from_store(self):
        with self.__sub_store_path.open(encoding='utf-8', mode='r') as st:
            go_to_store_point(st, FilePoints.RECORDS_START)
            string_data = st.readline()
            while string_data:
                data_list = [field for field in string_data.split() if field]
                data_record = Record(self.spec, *data_list)
                yield data_record

        return

    @staticmethod
    def __check_record_to_condition(record: Record, **conditions):
        result = True
        for cond_name, cond_value in conditions.items():
            if record[cond_name] != str(cond_value):
                result = False
        return result

