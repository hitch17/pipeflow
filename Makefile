.PHONY: test

test:
	python -m unittest

push-to-pypitest:
	python setup.py sdist upload -r pypitest

puth-to-pypi:
	python setup.py sdist upload -r pypi
