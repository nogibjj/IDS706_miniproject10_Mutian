install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main test_*.py

format:	
	black *.py 
lint:
	pylint --disable=R,C test_main.py

		
all: install format lint test 