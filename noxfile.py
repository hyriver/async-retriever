import nox


@nox.session
def tests(session):
    session.install(".[test]")
    session.run("coverage", "erase")
    session.run("coverage", "run", "-m", "ward")
    session.run("pytest")
    session.run("coverage", "report")
    session.run("coverage", "html")


@nox.session
def lint(session):
    session.run("pre-commit", "run", "--all-files", external=True)
