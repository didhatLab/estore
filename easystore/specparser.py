import abc

from typing import List, Dict

from easystore.utilis import SpecInfo


class AbstractSpecParser(metaclass=abc.ABCMeta):

    def parse_spec_string(self, spec: str):
        raise NotImplementedError()

    def parse_spec_list(self, spec: List[str]):
        raise NotImplementedError()


class SpecParser(AbstractSpecParser):

    def parse_spec_string(self, spec: str) -> SpecInfo:
        return self._parse_spec_string(spec)

    def parse_spec_list(self, spec: List[str]) -> SpecInfo:
        return self._parse_spec_list(spec)

    @staticmethod
    def _parse_spec_list(spec: List[str]) -> SpecInfo:
        fields_names = []
        fields_configs: Dict[str, List[str]] = {}

        for field in spec:
            field_name, *field_params = field.replace("[", " ").replace("]", "").split(" ")
            fields_names.append(field_name)
            fields_configs[field_name] = field_params

        return SpecInfo(fields=fields_names, fields_configs=fields_configs)

    def _parse_spec_string(self, spec: str) -> SpecInfo:

        spec_list = spec.split(" ")
        return self._parse_spec_list(spec_list)

