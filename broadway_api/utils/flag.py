import re
import os
import sys
import json
import argparse


class Flag:
    def __init__(
        self, name, _type, flag_name, env, config_name, required, default=None
    ):
        self.name = name
        self.type = _type
        self.flag_name = flag_name
        self.env = env
        self.config_name = config_name
        self.required = required
        self.default = default


# set of flags
class FlagSet:
    FLAG_PATTERN = r"^[a-zA-Z0-9\-]+$"
    INTERNAL_FLAG_CONFIG = "__config"

    @staticmethod
    def __bool(v):
        if v.lower() in ["t", "true", "y", "yes", "1"]:
            return True
        elif v.lower() in ["f", "false", "n", "no", "0"]:
            return False
        else:
            raise argparse.ArgumentTypeError("expecting boolean value")

    def __init__(self, config_parser=json.loads):
        """
        :params config_parser: should take a string and return dict
        """
        self.flags = {}
        self.name_set = set()  # set of flag names
        self.config_parser = config_parser
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument(FlagSet.INTERNAL_FLAG_CONFIG, nargs="?")

    def add_flag(
        self,
        canon,
        _type,
        default=None,
        aliases=[],
        env=None,
        config_name=None,
        required=False,
        help=None,
    ):
        """
        Add a flag to the flag set

        :param canon: canonical name, used in config file, length > 1
        :param type: type of the flag value
        :param default:
            default value. default != None implies required == False.
            if type is bool, default will be set to False if not set
        :param aliases: alias for the canonical name
        :param env: environment variable associated to it
        :param config_name: name used in config file
        :param required: whether the flag is required
        :param help: an optional help message
        """

        # preprocessing
        assert (
            default is None or not required
        ), "cannot provide default value for a required flag"
        assert (
            len(set([canon] + aliases).intersection(self.name_set)) == 0
        ), "duplicated flag name in {}".format([canon] + aliases)
        assert default is None or isinstance(
            default, _type
        ), "default is not {}".format(str(_type))

        assert (
            re.match(FlagSet.FLAG_PATTERN, canon) is not None
        ), "invalid flag name {}".format(canon)

        for alias in aliases:
            assert (
                re.match(FlagSet.FLAG_PATTERN, alias) is not None
            ), "invalid flag name {}".format(alias)

        # generate argparse config
        flag_names = ["--" + canon]

        kwargs = {"type": _type}

        # replace - with _ for dest
        if "-" in canon:
            kwargs["dest"] = dest = canon.replace("-", "_")
        else:
            dest = canon

        if help is not None:
            if default is not None:
                kwargs["help"] = "{}. default value '{}'".format(help, default)
            else:
                kwargs["help"] = help
        elif default is not None:
            kwargs["help"] = "default value '{}'".format(default)

        for alias in aliases:
            if len(alias) == 1:
                flag_names.append("-" + alias)
            else:
                flag_names.append("--" + alias)

        if _type is bool:
            kwargs["type"] = _type = FlagSet.__bool
            kwargs["nargs"] = "?"
            kwargs["const"] = True  # if used as --flag, true is stored

        self.parser.add_argument(*flag_names, **kwargs)
        self.flags[dest] = Flag(
            dest, _type, flag_names[0], env, config_name, required, default
        )

    # parse arguments from environment variable
    # exits and spring help message if an error is encountered
    def parse_env(self):
        var = {}

        for name, flag in self.flags.items():
            if flag.env is not None and flag.env in os.environ:
                v = os.environ[flag.env]
                if v != "":
                    try:
                        var[flag.name] = flag.type(v)
                    except Exception as e:
                        self.parser.error(
                            "faield to parse environment variable {}: '{}': {}".format(
                                flag.env, v, repr(e)
                            )
                        )

        return var

    # parse argument from command line
    # exits and spring help message if an error is encountered
    def parse_cmdline(self, args):
        var = vars(self.parser.parse_args())
        res = {}

        # remove None variables
        for k, v in var.items():
            if v is not None:
                res[k] = v

        return res

    def parse_config(self, config_file):
        with open(config_file, "rb") as f:
            var = self.config_parser(f.read())
            res = {}

            for name, flag in self.flags.items():
                if flag.config_name is not None and flag.config_name in var:
                    res[name] = var[flag.config_name]

            return res

    # priority: flag > environment var > config file
    def parse(self, args=sys.argv[1:]):
        cmdline_args = self.parse_cmdline(args)
        env_args = self.parse_env()

        if FlagSet.INTERNAL_FLAG_CONFIG in cmdline_args:
            config_args = self.parse_config(cmdline_args[FlagSet.INTERNAL_FLAG_CONFIG])
            del cmdline_args[FlagSet.INTERNAL_FLAG_CONFIG]
        else:
            config_args = {}

        # merge three dicts
        args = config_args.copy()
        args.update(env_args)
        args.update(cmdline_args)

        # fill in default values
        for k, v in self.flags.items():
            if k not in args:
                args[k] = v.default

        # check if remaining unset flags are required
        for flag in self.flags.values():
            if args[flag.name] is None and flag.required:
                self.parser.error(
                    "flag '{}' is required but not given".format(flag.name)
                )

        return args
