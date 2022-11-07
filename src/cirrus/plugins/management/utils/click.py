import click


class VariableFile(click.File):
    name = "variable file"

    def convert(self, value, param, ctx):
        import shlex

        f = super().convert(value, param, ctx)

        env = {}
        for line in f.readlines():
            name, val = line.split("=")
            val = shlex.split(val)

            if len(val) != 1:
                self.fail(f"Malformed variable file: {value}", param, ctx)

            env[name] = val[0]

        f.close()

        return env


class Variable(click.ParamType):
    name = "key/val pair"

    def convert(self, value, param, ctx):
        print(22, value)
        print(33, param)
        return {value[0]: value[1]}


def merge_vars1(ctx, param, value):
    env = {}
    for _vars in value:
        env.update(_vars)
    return env


def merge_vars2(ctx, param, value):
    env = ctx.params.pop("additional_variable_files", {})
    for key, val in value:
        env[key] = val
    return env


def additional_variables(func):
    func = click.argument(
        "additional_variable_files",
        nargs=-1,
        type=VariableFile(),
        callback=merge_vars1,
        is_eager=True,
    )(func)
    return click.option(
        "-x",
        "--var",
        "additional_variables",
        nargs=2,
        multiple=True,
        # type=Variable(),
        callback=merge_vars2,
        help="Additional templating variables",
    )(func)


def silence_templating_errors(func):
    return click.option(
        "--silence-templating-errors",
        is_flag=True,
    )(func)
