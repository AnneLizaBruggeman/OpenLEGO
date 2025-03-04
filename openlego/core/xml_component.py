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

This file contains the definition the `XMLComponent` class.
"""
from __future__ import absolute_import, division, print_function

import abc
import os
from abc import abstractmethod
from datetime import datetime

import numpy as np
from lxml import etree
from openmdao.api import Group, IndepVarComp, ExplicitComponent
from openmdao.vectors.vector import Vector
from typing import Optional, List, Union, Iterable, Tuple

from openlego.utils.xml_utils import xml_safe_create_element, xml_to_dict, xpath_to_param, param_to_xpath, xml_merge
from openlego.partials.partials import Partials

dir_path = os.path.dirname(os.path.realpath(__file__))


class XMLComponent(ExplicitComponent):
    """Abstract base class exposing an interface to use XML files for its in- and output.

    This subclass of `PromotingComponent` can automatically create ``OpenMDAO`` inputs and outputs based on given in-
    and output XML template files. For maximum flexibility it is possible to only specify inputs from an XML file and
    retain direct control over the definition of the outputs, or vice versa. It is also perfectly valid to add inputs
    even when an XML file is used to generate a set of inputs, or outputs when an XML file it used to generate outputs.
    It is even possible to generate in- and/or output parameters based on more than one XML file.

    This class exposes the functions `set_inputs_from_xml()` and `set_outputs_from_xml()` for this purpose. Lists of all
    parameters obtained from XML files are stored by this class for later inspection.

    The `solve_nonlinear()` method of the `Component` class is implemented to wrap the XML related operations such as
    reading in- and output data from the corresponding XML files during execution and storing it in this `Component`'s
    parameter dictionaries.

    A new abstract method is defined by this class, `execute()`, which assumes the role of the `solve_nonlinear()`
    function, in essence. A specific case of this class should implement this method to perform the actual calculations
    of an analysis tool using XML in- and/or output.

    Attributes
    ----------
        inputs_from_xml, outputs_from_xml, partials_from_xml : dict
            List of inputs, resp. outputs, resp. partials, taken from XML.

        data_folder : str('')
            Path to a folder in which to store data generated during the execution of this `XMLComponent`.

        keep_files : bool(False)
            Set to `True` to keep all temporary XML files generated by the `XMLComponent` during execution.

            This attribute is `False` by default, in which case all temporary in- and output XML files will be deleted
            after they are no longer needed by this component.

        base_file : str, optional
            Path to an XML file to keep up-to-date with the latest data from executions.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self,
                 input_xml=None,            # type: Optional[Union[str, etree._ElementTree]]
                 output_xml=None,           # type: Optional[Union[str, etree._ElementTree]]
                 partials_xml=None,         # type: Optional[Union[str, etree._ElementTree]]
                 data_folder='',            # type: str
                 keep_files=False,          # type: bool
                 base_file=None             # type: Optional[str]
                 ):
        # type: (...) -> None
        """Initialize the `XMLComponent`.

        Parameters
        ----------
            input_xml, output_xml, partials_xml : str or :obj:`etree._ElementTree`, optional
                Paths to or an `etree._ElementTree` of input, resp. output, resp. partial, XML files.

            data_folder : str
                Path to the folder in which to store temporary data files generated by this `XMLComponent`.

            keep_files : bool(False)
                Set to `True` to keep the temporary XML files after they are no longer needed.

            base_file : str, optional
                Path to a base XML file to keep up-to-date with all latest data from this `XMLComponent`.
        """
        super(XMLComponent, self).__init__()

        self.inputs_from_xml = dict()
        self.outputs_from_xml = dict()
        self.partials_from_xml = dict()

        if input_xml is not None:
            self.set_inputs_from_xml(input_xml)

        if output_xml is not None:
            self.set_outputs_from_xml(output_xml)

        if partials_xml is not None:
            self.declare_partials_from_xml(partials_xml)

        self.data_folder = data_folder
        self.keep_files = keep_files
        self.base_file = base_file

    def set_inputs_from_xml(self, input_xml):
        # type: (Union[str, etree._ElementTree]) -> None
        """Set inputs to the `Component` based on an input XML template file.

        Parameter names correspond to their XML elements' full XPaths, converted to valid ``OpenMDAO`` names using the
        `xpath_to_param()` method.

        Parameters
        ----------
            input_xml : str or :obj:`etree._ElementTree`
                Path to or an `etree._ElementTree` of an input XML file.
        """
        self.inputs_from_xml.clear()
        for xpath, value in xml_to_dict(input_xml).items():
            name = xpath_to_param(xpath)
            self.inputs_from_xml.update({name: value})

    def set_outputs_from_xml(self, output_xml):
        # type: (Union[str, etree._ElementTree]) -> None
        """Set outputs to the `Component` based on an output XML template file.

        Parameter names correspond to their XML elements' full XPaths, converted to valid ``OpenMDAO`` names using the
        `xpath_to_param()` method.

        Parameters
        ----------
            output_xml : str or :obj:`etree._ElementTree`
                Path to or an `etree._ElementTree` of an output XML file.
        """
        self.outputs_from_xml.clear()
        for xpath, value in xml_to_dict(output_xml).items():
            name = xpath_to_param(xpath)
            self.outputs_from_xml.update({name: value})

    def declare_partials_from_xml(self, partial_xml):
        # type: (Union[str, etree._ElementTree]) -> None
        """Declare partials to the `Component` based on a partials XML template file.

        Parameters
        ----------
            partial_xml : str or :obj:`etree._ElementTree`
                Path to or an `etree._ElementTree` of a partials XML file.
        """
        self.partials_from_xml.clear()
        if partial_xml is not None:
            partials = Partials(partial_xml)
            self.partials_from_xml = partials.get_partials().copy()

    @property
    def variables_from_xml(self):
        # type: () -> dict
        """:obj:`dict`: Dictionary of all XML inputs and outputs."""
        variables = self.inputs_from_xml.copy()
        variables.update(self.outputs_from_xml.copy())
        return variables

    def setup(self):
        for name, value in self.inputs_from_xml.items():
            if (not isinstance(value, float) and not isinstance(value, np.ndarray)) or \
                        (isinstance(value, float) and np.isnan(value)) or \
                        (isinstance(value, np.ndarray) and any(np.isnan(value))):
                self.add_discrete_input(name, value)
            else:
                self.add_input(name, value)

        for name, value in self.outputs_from_xml.items():
            if (not isinstance(value, float) and not isinstance(value, np.ndarray)) or \
                        (isinstance(value, float) and np.isnan(value)) or \
                        (isinstance(value, np.ndarray) and any(np.isnan(value))):
                self.add_discrete_output(name, value)
            else:
                # Use the value stored in the input.xml as a reference value
                if isinstance(value, np.ndarray):
                    ref = value.mean()
                else:
                    ref = value
                if ref == 0.:
                    ref = 1.

                self.add_output(name, value, ref=ref)

        if self.partials_from_xml:
            for of, wrt in self.partials_from_xml.items():
                if of is not None and wrt is not None:
                    self.declare_partials(xpath_to_param(of), [xpath_to_param(_wrt) for _wrt in wrt.keys()])
        else:
            self.declare_partials('*', '*', method='fd', step_calc='rel')
            # if self.outputs_from_xml and self.inputs_from_xml:
            #     for src in self.outputs_from_xml.keys():
            #         self.declare_partials(src, self.inputs_from_xml.keys(), method='fd')

    @abstractmethod
    def execute(self, input_xml=None, output_xml=None):
        # type: (Optional[str], Optional[str]) -> None
        """Execute the tool using the given input XML file. Write the results to the given output XML file.

        Parameters
        ----------
            input_xml, output_xml : str, optional
                Path to the input, resp. output, XML file.
        """
        raise NotImplementedError

    @abstractmethod
    def linearize(self, input_xml=None, partials_xml=None):
        # type: (Optional[str], Optional[str]) -> None
        """Compute the partials of a tool using the given XML file. Write the results to the given partials XML file.

        Parameters
        ----------
            input_xml, partials_xml : str, optional
                Path to the input, resp. partials, XML file.
        """
        raise NotImplementedError

    def generate_file_names(self):
        # type: () -> Tuple[str, str, str]
        """Generate temporary file names for the input, output, and partials XML files.

        Returns
        -------
            str
                Input XML file path.

            str
                Output XML file path.

            str
                Partials XML file path.

        """
        salt = datetime.now().strftime('%Y%m%d%H%M%f')
        input_xml = os.path.join(self.data_folder, self.name + '_in_%s.xml' % salt)
        output_xml = os.path.join(self.data_folder, self.name + '_out_%s.xml' % salt)
        partials_xml = os.path.join(self.data_folder, self.name + '_partials_%s.xml' % salt)

        return input_xml, output_xml, partials_xml

    def write_input_file(self, file, inputs, discrete_inputs=None):
        # type: (Union[str, etree._ElementTree], Vector, Vector) -> None
        """Write the current input values to an input XML file.

        Parameters
        ----------
            file : str or :obj:`etree._ElementTree`
                Path to or :obj:`etree._ElementTree` of an input XML file.

            inputs : Vector
                Input vector of this `Component`.

            discrete_inputs : Vector
                Discrete input vector of this `Component`.
        """
        # Create new root element and an ElementTree
        root = etree.Element(param_to_xpath(list(self.inputs_from_xml)[0]).split('/')[1])
        doc = etree.ElementTree(root)

        # Convert all XML param names to XPaths and add new elements to the tree correspondingly
        for param in self.inputs_from_xml:
            if param in inputs:
                xml_safe_create_element(doc, param_to_xpath(param), inputs[param])
            elif param in discrete_inputs:
                xml_safe_create_element(doc, param_to_xpath(param), discrete_inputs[param])

        # Write the tree to an XML file
        doc.write(file, pretty_print=True, xml_declaration=True, encoding='utf-8')


    def read_outputs_file(self, file, outputs, discrete_outputs=None):
        # type: (Union[str, etree._ElementTree], Vector, Vector) -> None
        """Read the outputs from a given XML file and store them in this `Component`'s variables.

        Parameters
        ----------
            file : str or :obj:`etree._ElementTree`
                Path to or :obj:`etree._ElementTree` of an output XML file.

            outputs : Vector
                Output vector of this `Component`.

            discrete_outputs : Vector
                Discrete output vector of this `Component`.
        """
        # Extract the results from the output xml
        for xpath, value in xml_to_dict(file).items():
            name = xpath_to_param(xpath)
            if name in self.outputs_from_xml and name in outputs:
                outputs[name] = value
            elif name in self.outputs_from_xml and name in discrete_outputs:
                discrete_outputs[name] = value

    def read_partials_file(self, file, partials):
        # type: (Union[str, etree._ElementTree], Vector) -> None
        """Read the partials from a given XML file and store them in this `Component`'s variables.

        Parameters
        ----------
            file : str or :obj:`etree._ElementTree`
                Path to or :obj:`etree._ElementTree` of a partials XML file.

            partials : Vector
                Partials vector of this `Component`.

        """
        _partials = Partials(file)
        for of, wrts in _partials.get_partials().items():
            for wrt, val in wrts.items():
                of = xpath_to_param(of)
                wrt = xpath_to_param(wrt)
                if (of, wrt) in partials:
                    try:
                        partials[of, wrt] = val
                    except Exception as e:
                        print(e.message)

    def compute(self, inputs, outputs, discrete_inputs=None, discrete_outputs=None):
        # type: (Vector, Vector) -> None
        """Write the input XML file, call `execute()`, and read the output XML file to obtain the results.

        Parameters
        ----------
            inputs : `Vector`
                Input parameters.

            outputs : `Vector`
                Output parameters.
        """

        input_xml, output_xml, _ = self.generate_file_names()

        if self.inputs_from_xml:
            self.write_input_file(input_xml, inputs, discrete_inputs)
            if self.base_file is not None:
                xml_merge(self.base_file, input_xml)

        # Call execute
        if self.base_file is not None:
            self.execute(self.base_file, output_xml)
            xml_merge(self.base_file, output_xml)
        else:
            self.execute(input_xml, output_xml)

        # If files should not be kept, delete the input XML file
        if not self.keep_files:
            try:
                os.remove(input_xml)
            except OSError:
                pass

        if self.outputs_from_xml:
            self.read_outputs_file(output_xml, outputs, discrete_outputs)

            # If files should not be kept, delete the output XML file
            if not self.keep_files:
                try:
                    os.remove(output_xml)
                except OSError:
                    pass

    def compute_partials(self, inputs, partials):
        # type: (Vector, Vector) -> None
        """Write the input XML file, call `linearize()`, and read the sensitivities from the resulting XML file.

        Parameters
        ----------
            inputs : `Vector`
                Input parameters.

            partials: `Vector`
                Partials.
        """
        if self.partials_from_xml:
            input_xml, _, partials_xml = self.generate_file_names()

            self.write_input_file(input_xml, inputs)
            self.linearize(input_xml, partials_xml)

            if not self.keep_files:
                try:
                    os.remove(input_xml)
                except OSError:
                    pass

            self.read_partials_file(partials_xml, partials)

            if not self.keep_files:
                try:
                    os.remove(partials_xml)
                except OSError:
                    pass

    def xml_params_as_indep_vars(self, group, params, values, aliases=None):
        # type: (Group, List[str], Union[np.ndarray, Iterable], Optional[List[str]]) -> None
        """Create `IndepVarComp`s for given input params of this `XMLComponent`.

        Parameters
        ----------
            group : :obj:`Group`
                `Group` to add the `IndepVarComp`s to.

            params : list of str
                List of param names. These need to exist in this `XMLComponent`.

            values : :obj:`np.ndarray` or list of numbers
                List of (initial) values for all `IndepVarComp`s.

            aliases : list of str, optional
                List of aliases (promoted names) to give the `IndepVarComp`s.
        """
        if len(params) != len(values) or (aliases is None and len(params) != len(aliases)):
            raise ValueError('number of params, values and optionally aliases needs to be the same')

        for param in params:
            if param not in self.inputs_from_xml:
                raise ValueError('at least one param given is not a param of this XMLComponent (%s)' % param)

        for index, param in enumerate(params):
            if aliases is None:
                alias = 'INDEP_' + param_to_xpath(param).split('/')[-1].split('[')[0]
            else:
                alias = aliases[index]

            group.add(alias, IndepVarComp(alias, val=values[index]), promotes=[alias])
            group.connect(alias, param)
