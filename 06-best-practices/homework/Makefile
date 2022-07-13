unit_tests: setup
	pytest ./tests/test_batch.py

integration_tests: unit_tests setup
	bash ./tests/run_integration_test.sh

setup:
	pipenv install --dev
	-pipenv shell

