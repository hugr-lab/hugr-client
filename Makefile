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

lab:
	source .venv/bin/activate && \
		uv pip install jupyterlab ipykernel && \
		uv pip install jupyterlab-lsp python-lsp-server[all] && \
		uv pip install python-lsp-server[rope] pylsp-mypy pylsp-rope && \
		uv pip install -e . && \
		python -m ipykernel install --user --name=venv --display-name="Python (hugr)" && \
		jupyter lab --no-browser --port=8888