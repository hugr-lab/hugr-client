venv:
	uv venv
	source .venv/bin/activate && uv pip install -e .
run:
	source .venv/bin/activate && python example.py

clean:
	rm -rf .venv
	rm -rf __pycache__
	rm -rf *.pyc
	rm -rf *.pyo
	rm -rf *.pyd
	rm -rf *.egg-info

DUCKDB_KERNEL_DIR := $(CURDIR)/../duckdb-kernel

lab: install-lab
	source .venv/bin/activate && \
		jupyter lab --no-browser --port=8888

install-lab:
	source .venv/bin/activate && \
		uv pip install jupyterlab ipykernel && \
		uv pip install -e . && \
		uv pip install -e $(DUCKDB_KERNEL_DIR)/extensions/jupyter/perspective-viewer/ && \
		python -m ipykernel install --user --name=venv --display-name="Python (hugr)"