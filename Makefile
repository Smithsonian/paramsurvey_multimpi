.PHONY: unit unit_coverage integration integration_coverage clean_coverage test

COV=--cov-append --cov-branch --cov paramsurvey_multimpi

unit:
	PYTHONPATH=. pytest -v -v tests/unit

unit_coverage:
	PYTHONPATH=. pytest ${COV} -v -v tests/unit

integration:
	PYTHONPATH=. pytest -v -v tests/integration

integration_coverage:
	PYTHONPATH=. pytest ${COV} -v -v tests/integration
	PYTHONPATH=. TEST_GENERIC=multiprocessing_test pytest ${COV} -v -v tests/integration/test-generic.py
	#PYTHONPATH=. TEST_GENERIC=multiprocessing_test pytest ${COV} -v -v tests/integration/test-generic.py

clean_coverage:
	rm -f .coverage

test: unit integration

test_coverage: clean_coverage unit_coverage integration_coverage

distclean:
	rm -rf dist/

distcheck: distclean
	python ./setup.py sdist
	twine check dist/*

dist: distclean
	echo "reminder, you must have tagged this commit or you'll end up failing"
	echo "  finish the CHANGELOG"
	echo "  git tag v0.x.x"
	echo "  git push --tags"
	python ./setup.py sdist
	twine check dist/*
	twine upload dist/* -r pypi
