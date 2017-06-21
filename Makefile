.PHONY: test

test:
	python -m unittest discover -s test

push-to-pypitest:
	python setup.py sdist upload -r pypitest

push-to-pypi:
	python setup.py sdist upload -r pypi
