# -*- coding: utf-8 -*-
import abc
import gzip
import zipfile

import os


class CompressClass(object, metaclass=abc.ABCMeta):
    def __init__(self):
        super().__init__()

    @abc.abstractmethod
    def compress_file(self, file_path):
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def decompress_file(self, file_path, encoding):
        raise NotImplementedError("Abstract method")


class GzipCompressClass(CompressClass):
    def __init__(self):
        super().__init__()

    def compress_file(self, file_path):
        file = "/tmp/" + file_path.split("/")[-1]
        with open(file, "rb") as r:
            with gzip.open(file + ".gz", "wb") as f:
                f.write(r.read())

        os.remove(file)

        return file_path + ".gz"

    def decompress_file(self, file_path):
        file_name = file_path.split("/")[-1]
        source = "/tmp/" + file_name
        destination = file_name[:-3] if ".gz" in file_name else file_name + "01"
        with gzip.open(source, "rb") as f:
            with open("/tmp/" + destination, "wb") as w:
                w.write(f.read())

        os.remove("/tmp/" + file_name)

        return destination


class ZipCompressClass(CompressClass):
    def __init__(self):
        super().__init__()

    def compress_file(self, file_path):
        file = "/tmp/" + file_path.split("/")[-1]
        zip = zipfile.ZipFile(file + ".zip", "w", compression=zipfile.ZIP_DEFLATED)
        zip.write(file, arcname=file_path.split("/")[-1])
        zip.close()

        os.remove(file)

        return file_path + ".zip"

    def decompress_file(self, file_path):
        file_name = file_path.split("/")[-1]
        source = "/tmp/" + file_name
        zip = zipfile.ZipFile(source, "r", compression=zipfile.ZIP_DEFLATED)

        # Ensuring that the zip contains only a single file
        if (len(zip.namelist())) > 1:
            raise Exception("Zip file must contain a single file")

        for name in zip.namelist():
            with open("/tmp/" + name, "wb") as f:
                f.write(zip.read(name))

        os.remove("/tmp/" + file_name)

        return name


def get_compression_types():
    return {"gzip": GzipCompressClass, "zip": ZipCompressClass}
