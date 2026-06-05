from invoke import task, Collection

ns = Collection()


@task
def fmt(c):
    """Format code using ruff formatter.

    Usage: inv dev.fmt
    """
    c.run("uv run -m ruff format .", pty=True)


@task
def fix(c):
    """Fix code using ruff fixer.

    Usage: inv dev.fix
    """
    c.run("uv run -m ruff check --fix .", pty=True)


@task
def check(c):
    """Run ruff checks.

    Usage: inv dev.check
    """
    c.run("uv run -m ruff check .", pty=True)


@task(aliases=["pcin"])
def pc_install(c):
    """Install pre-commit hooks.

    Usage: inv dev.pc-install (or inv dev.pcin)
    """
    c.run("uv run pre-commit install", pty=True)


@task(aliases=["pcrun"])
def pc_run(c):
    """Run pre-commit hooks on all files.

    Usage: inv dev.pc-run (or inv dev.pcrun)
    """
    c.run("uv run pre-commit run --all-files", pty=True)


development = Collection("dev")
development.add_task(fmt)
development.add_task(fix)
development.add_task(check)
development.add_task(pc_install)
development.add_task(pc_run)

ns.add_collection(development)
