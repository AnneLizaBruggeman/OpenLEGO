#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Copyright 2018 D. de Vries

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This file contains the definition of the AbstractDiscipline interface class.
"""
from __future__ import absolute_import, division, print_function

import abc
import inspect
import os
import sys

from six import string_types

from openlego.partials.partials import Partials
from openlego.utils.xml_utils import xml_merge
from openlego.utils.general_utils import get_args


class AbstractDiscipline(object):
    """Defines the common interface for all disciplines within ``OpenLEGO``."""
    __metaclass__ = abc.ABCMeta

    @property
    def name(self):
        # type: () -> str
        """:obj:`str`: Name of this discipline."""
        return self.__class__.__name__

    @property
    def path(self):
        # type: () -> str
        """:obj:`str`: Path at which this discipline resides."""
        return os.path.dirname(inspect.getfile(self.__class__))

    @property
    def in_file(self):
        # type: () -> str
        """:obj:`str`: Path of the template input XML file of this discipline."""
        return os.path.join(self.path, self.name + '-input.xml')

    @property
    def out_file(self):
        # type: () -> str
        """:obj:`str`: Path of the template output XML file of this discipline."""
        return os.path.join(self.path, self.name + '-output.xml')

    @property
    def partials_file(self):
        # type: () -> str
        """:obj:`str`: Path of the partials XML file of this discipline."""
        return os.path.join(self.path, self.name + '-partials.xml')

    @property
    def supplies_partials(self):
        # type: () -> bool
        """Set to True to indicate this discipline supplies gradients."""
        return False

    @abc.abstractmethod
    def generate_input_xml(self):
        # type: () -> string_types
        """Generate the template input XML for this discipline.

        This method should be implemented to define the input template of a specific discipline.

        Returns
        -------
            string_types
                String representation of the template input XML.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def generate_output_xml(self):
        # type: () -> string_types
        """Generate the template output XML for this discipline.

        This method should be implemented to define the output template of a specific discipline.

        Returns
        -------
            string_types
                String representation of the template output XML file.
        """
        raise NotImplementedError

    def generate_partials_xml(self):
        # type: () -> string_types
        """Generate the template partials XML file for this discipline.

        This method should be implemented to define for which inputs this discipline can provide
        the sensitivities.

        Returns
        -------
            string_types
                String representation of the template partials XML file.
        """
        return Partials().get_string()

    def deploy(self):
        # type: () -> None
        """Deploy this discipline's template in-/output, partials XML files and its information
        JSON file."""
        with open(self.in_file, 'wb') as f:
            f.write(self.generate_input_xml())
        with open(self.out_file, 'wb') as f:
            f.write(self.generate_output_xml())
        with open(self.partials_file, 'wb') as f:
            f.write(self.generate_partials_xml())

    @staticmethod
    @abc.abstractmethod
    def execute(in_file, out_file):
        # type: (str, str) -> None
        """Execute this discipline with the given in- and output XML files.

        This method should be implemented to define the execution of a specific discipline.

        Parameters
        ----------
            in_file : str
                Path to the input XML file.

            out_file : str
                Path to the output XML file.
        """
        raise NotImplementedError

    @staticmethod
    def linearize(in_file, partials_file):
        # type: (str, str) -> None
        """Compute the sensitivities of a given input XML file and write them to a given partials
        XML file.

        This method should be implemented to define the linearization of a specific discipline. By
        default a discipline is considered a 'black box', and no sensitivities are provided.

        Parameters
        ----------
            in_file : str
                Path to the input XML file.

            partials_file : str
                Path to the sensitivities XML file.
        """
        Partials().write(partials_file)

    def run_tool(self, sys_argv):
        class_name = self.__class__.__name__
        class_name_lc = class_name.lower()

        args = get_args(sys_argv)

        out_file = args.out_file.format(class_name_lc)

        if args.test:
            in_file = '__test__{}_input.xml'.format(class_name_lc)
            with open(in_file, 'wb') as f:
                f.write(self.generate_input_xml())
            self.execute(in_file, out_file)
            sys.stdout.write('Executed test run of {}.py with input file "{}" and output file '
                             '"{}".\n'.format(class_name, in_file, out_file))
        else:
            in_file = args.in_file
            if not os.path.isfile(in_file):
                raise AssertionError('could not find the input file "{}" in the folder.'
                                     .format(in_file))
            self.execute(in_file, out_file)
            sys.stdout.write('Executed run of {}.py with input file "{}" and output file "{}".\n'
                             .format(class_name, in_file, out_file))
        if args.merge_files:
            xml_merge(in_file, out_file, out_file)
