.PHONY: test

test:
	clj -A\:test

repl:
	clj -R:repl bin/repl.clj

deploy: test
	clj -Spom
	mvn deploy
