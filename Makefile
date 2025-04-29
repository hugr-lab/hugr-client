venv:
	uv venv
	source .venv/bin/activate && uv pip install -e .

run:
	source .venv/bin/activate && python example.py