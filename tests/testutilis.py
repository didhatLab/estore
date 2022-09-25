import pathlib


def delete_temp_files_v3(*files: pathlib.Path):
    for file in files:
        file.unlink()
