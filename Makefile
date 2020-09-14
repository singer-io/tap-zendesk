.PHONY: test
.DEFAULT_GOAL := test

test:
	pylint tap_zendesk -d missing-docstring,invalid-name,line-too-long,too-many-locals,too-few-public-methods,fixme,stop-iteration-return,too-many-branches,useless-import-alias,no-else-return,logging-not-lazy
	nosetests test/unittests
