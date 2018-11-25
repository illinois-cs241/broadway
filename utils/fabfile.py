from fabric import Connection, task


@task
def install(connection):
    ls_res = connection.run("ls")
    print(ls_res)
    # with connection.cd("deploy"):
