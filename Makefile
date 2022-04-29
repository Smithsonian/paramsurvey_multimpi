.PHONY: unit unit_coverage integration integration_coverage clean_coverage test

COV=--cov-append --cov-branch --cov paramsurvey_multimpi

unit:
	# hint: PYTEST_STDERR_VISIBLE=-s
	PYTHONPATH=. pytest -v -v ${PYTEST_STDERR_VISIBLE} tests/unit

unit_coverage:
	PYTHONPATH=. pytest ${COV} -v -v ${PYTEST_STDERR_VISIBLE} tests/unit

integration:
	# hint: PYTEST_STDERR_VISIBLE=-s
	PYTHONPATH=. pytest -v -v -s tests/integration
	PYTHONPATH=. TEST_GENERIC=multiprocessing_test pytest -v -v ${PYTEST_STDERR_VISIBLE} tests/integration/test-generic.py
	PYTHONPATH=.:tests/integration TEST_GENERIC=ray_test bash tests/integration/test-ray.sh -v -v ${PYTEST_STDERR_VISIBLE} tests/integration/test-generic.py

integration_coverage:
	PYTHONPATH=. pytest ${COV} -v -v tests/integration
	PYTHONPATH=. TEST_GENERIC=multiprocessing_test pytest ${COV} -v -v ${PYTEST_STDERR_VISIBLE} tests/integration/test-generic.py
	PYTHONPATH=.:tests/integration TEST_GENERIC=ray_test bash tests/integration/test-ray.sh ${COV} -v -v ${PYTEST_STDERR_VISIBLE} tests/integration/test-generic.py

clean_coverage:
	rm -f .coverage

test: unit integration

test_coverage: clean_coverage unit_coverage integration_coverage

check_action:
	python -c 'import yaml, sys; print(yaml.safe_load(sys.stdin))' < .github/workflows/test-all.yml

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
