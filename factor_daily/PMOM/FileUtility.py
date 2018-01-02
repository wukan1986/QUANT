# -*- coding: utf-8 -*-
import os

def create_dir(path):
    path = path.strip()
    path = path.rstrip("\\")
    if not os.path.exists(path):
        os.mkdir(path)


if __name__ == "__main__":
    create_dir('E:/data/pmom/pmom940')