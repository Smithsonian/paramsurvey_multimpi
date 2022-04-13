.PHONY: unit unit_coverage clean_coverage test

unit:
	PYTHONPATH=. pytest -v -v tests/unit

unit_coverage:
	PYTHONPATH=. pytest --cov-append --cov-branch --cov paramsurvey_multimpi -v -v tests/unit

clean_coverage:
	rm -f .coverage

test: unit

test_coverage: clean_coverage unit_coverage 

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
