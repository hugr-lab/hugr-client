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

lab: install-lab
	source .venv/bin/activate && \
		HUGR_SPOOL_EXTRA_DIRS=hugr-client \
		jupyter lab --no-browser --port=8888

install-lab:
	source .venv/bin/activate && \
		uv pip install jupyterlab ipykernel && \
		uv pip install -e . && \
		uv pip install "hugr-perspective-viewer>=0.3.2" && \
		python -m ipykernel install --user --name=venv --display-name="Python (hugr)"