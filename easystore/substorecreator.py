import abc
import pathlib

from typing import List

from easystore.specparser import SpecParser
from easystore.utilis import SpecInfo


class AbstractSubStoreCreator(metaclass=abc.ABCMeta):

    def create_sub_store(self, name: str, spec: List[str]):
        raise NotImplementedError()


class SubStoreCreator(AbstractSubStoreCreator):

    def __init__(self, main_store_path: pathlib.Path):
        self.main_path = main_store_path

    def create_sub_store(self, name: str, spec: List[str]):
        self.__create_sub_store(name, spec)

    def __create_sub_store(self, name: str, spec: List[str]):
        sub_store_path = self.__create_path_for_sub_store(name)
        meta_sub_store_path = self.__create_path_for_meta_store(name)
        self.__create_sub_store_file(sub_store_path)
        self.__create_meta_file_for_store(meta_sub_store_path, spec)

    @staticmethod
    def __create_sub_store_file(path: pathlib.Path):
        with path.open(mode="w") as st:
            st.write("#records")

        return True

    def __create_meta_file_for_store(self, path: pathlib.Path, spec: List[str]):
        spec_string = " ".join(spec)
        spec_info = SpecParser().parse_spec_list(spec)
        pk_sets = self.__create_pk_hashes_for_meta(spec_info)
        with path.open(mode="w") as stm:
            stm.write(f"{spec_string}\n"
                      f"#hashes\n"
                      f"{pk_sets}\n"
                      f"#endhashes")

    @staticmethod
    def __create_pk_hashes_for_meta(spec_info: SpecInfo) -> str:
        params_for_hashes = []
        for param_name, configs in spec_info.fields_configs.items():
            if "pk" in configs:
                params_for_hashes.append(param_name)
        return "\n".join(params_for_hashes)

    def __create_path_for_sub_store(self, name):
        new_path = self.main_path.parent / pathlib.Path(f"{name}.sbstore")
        return new_path

    def __create_path_for_meta_store(self, name):
        new_path = self.main_path.parent / pathlib.Path(f"{name}.sbstore.meta")
        return new_path
