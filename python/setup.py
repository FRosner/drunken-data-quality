#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Setup file for pyddq.

    This file was generated with PyScaffold 2.5.6, a tool that easily
    puts up a scaffold for your new Python project. Learn more under:
    http://pyscaffold.readthedocs.org/
"""

import sys
import os
import glob
import subprocess
from setuptools import setup, Command


class IntegrationTestCommand(Command):
    description = "A command to run integration tests"
    user_options = [("jar=", None, "Path to Drunken Data Quality jar")]
    jar = None
    addopts = None

    def initialize_options(self):
        pass

    def finalize_options(self):
        if self.addopts is None:
            exit("error: option addopts should be specified in setup.cfg")
        elif self.jar is None:
            exit("error: path to Drunken Data Quality jar should be specified")

    def run(self):
        for filename in glob.glob(os.path.join(self.addopts, "test_*.py")):
            subprocess.call([
                "spark-submit",
                "--driver-class-path",
                self.jar,
                filename
            ])


def setup_package():
    needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
    sphinx = ['sphinx'] if needs_sphinx else []
    setup(setup_requires=['six', 'pyscaffold>=2.5a0,<2.6a0'] + sphinx,
          use_pyscaffold=True,
          cmdclass={
              "integration_test": IntegrationTestCommand
          })


if __name__ == "__main__":
    setup_package()
