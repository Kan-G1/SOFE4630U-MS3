# Setup file for SmartMeter Dataflow workers.
# This installs any required dependencies on each worker node.

import subprocess
from distutils.command.build import build as _build
import setuptools


class build(_build):  # pylint: disable=invalid-name
    sub_commands = _build.sub_commands + [('CustomCommands', None)]


class CustomCommands(setuptools.Command):

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def RunCustomCommand(self, command_list):
        print('Running command: %s' % command_list)
        p = subprocess.Popen(
            command_list,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        stdout_data, _ = p.communicate()
        print('Command output: %s' % stdout_data)
        if p.returncode != 0:
            raise RuntimeError(
                'Command %s failed: exit code: %s' % (command_list, p.returncode)
            )

    def run(self):
        for command in CUSTOM_COMMANDS:
            self.RunCustomCommand(command)


# No extra dependencies needed for the smart meter pipeline —
# it only uses apache_beam which is already available.
CUSTOM_COMMANDS = []

REQUIRED_PACKAGES = []

setuptools.setup(
    name='smartmeter-dataflow',
    version='0.0.1',
    description='Smart Meter Dataflow processing pipeline.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    cmdclass={'build': build, 'CustomCommands': CustomCommands},
)
