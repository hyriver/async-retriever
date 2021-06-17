import nox


@nox.session
def tests(session):
    session.install(".[test]")
    session.run("coverage", "erase")
    session.run("coverage", "run", "-m", "ward")
    session.run("pytest")
    session.run("coverage", "xml", "-i")
    session.run("coverage", "report")
    session.run("coverage", "html")
