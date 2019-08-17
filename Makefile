.PHONY: test

test:
	clj -Atest

repl:
	clj -Rrepl bin/repl.clj

release: test
	clj -Spom
	mvn deploy
