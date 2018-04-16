import getpass
import os
import socket

import configobj
import validate

configspec = configobj.ConfigObj(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "configrc"),
    interpolation=False, list_values=False, _inspec=True)

configspec["username"] = "string(default=%s)" % getpass.getuser()
configspec["mongodb"]["user"] = "string(default=%s)" % getpass.getuser()

config = configobj.ConfigObj(
    infile=os.path.join(os.environ["HOME"], ".cumulusrc"),
    configspec=configspec)

config.validate(validate.Validator())
