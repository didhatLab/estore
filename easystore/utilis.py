import enum
import abc
import pathlib

from dataclasses import dataclass
from typing import Set, Dict, List, Optional, Any, Callable


class Validator(enum.Enum):
    pk = "pk_validator"


@dataclass
class SubStoreMetaData:
    sub_store_name: str
    pk_fields_sets: Dict[str, Set]


@dataclass(frozen=True)
class FilePoints:
    PK_SETS_START = "#hashes"
    PK_SETS_END = "#endhashes"
    RECORDS_START = "#records"
    SUB_STORES_START = "#substores"
    SUB_STORES_END = "#endsubstores"
    RECORDS_END = ""
    END = ""


def config2validator_type(config: str):
    return Validator[config]


@dataclass
class MetaInfo:
    fields: List[str]
    fields_config: Dict[str, List[Validator]]
    pk_sets: Dict[str, Set[int]]


@dataclass
class SpecInfo:
    fields: List[str]
    fields_configs: Dict[str, List[str]]


def go_to_store_point(file, point: FilePoints):
    line = file.readline()
    while line.replace("\n", "") != point:
        line = file.readline()

    return file


def read_data_until_point(file, point: FilePoints):
    line = file.readline()

    while line.replace("\n", "") != point:
        yield line
        line = file.readline()

    return


def temp_file2main(main_file: pathlib.Path, temp_file: pathlib.Path):
    main_name = main_file.name
    main_file.unlink()
    new_file = temp_file.rename(temp_file.parent / pathlib.Path(main_name))
    return new_file


class AbstractRecord(metaclass=abc.ABCMeta):
    pass


class Record(AbstractRecord):

    def __init__(self, sub_store_spec: List[str], *args, **kwargs):
        for field, value in zip(sub_store_spec, args):
            setattr(self, field, value)
        for field, value in kwargs.items():
            setattr(self, field, value)

    def __repr__(self):
        return "Record<{}>".format(self.__dict__)

    def __str__(self):
        return "StoreRecord"

    def __iter__(self):
        iters = [(key, value) for key, value in self.__dict__.items()]

        for field, value in iters:
            yield field, value

    def __getitem__(self, item) -> Optional[Any]:
        return self.__dict__.get(item, None)


def params2record(func: Callable[[object, Record], Any]):
    def decorator(obj, spec: List[str], *args, **names_params):
        record = Record(spec, *args, **names_params)
        return func(obj, record)

    return decorator
