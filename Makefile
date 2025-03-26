.PHONY: setup ais_stream pipeline_live flask_app cleanup run_all

SHELL := /bin/bash

setup:
	@echo "flask app setup..."
	python3 -m venv venv
	. source venv/bin/activate && pip install --upgrade pip
	. source venv/bin/activate && pip install -r requirements.txt

ais_stream:
	@echo "ais_stream setup..."
	. source venv/bin/activate && \
	python ais_stream.py

pipeline_live:
	@echo "Pipeline Dataflow (LIVE, collisions) ..."
	. source venv/bin/activate && \
	export $$(sed '/^ *#/d; /^$$/d' .env | xargs) && \
	python ais_pipeline/pipeline.py \
		--runner=DataflowRunner \
		--project=$$GOOGLE_CLOUD_PROJECT \
		--region=$$REGION \
		--staging_location=$$STAGING_LOCATION \
		--temp_location=$$TEMP_LOCATION \
		--job_name=$$JOB_NAME \
		--requirements_file=requirements.txt \
		--save_main_session \
		--setup_file=./setup.py

flask_app:
	@echo "Run Flask..."
	. venv/bin/activate && \
	export $(shell sed '/^ *#/d; /^$$/d' .env | xargs) && \
	flask run

run_all:
	@echo "Starting all services in tmux..."
	tmux new-session -d -s ship-collision 'bash -c ". venv/bin/activate && flask run"'
	tmux split-window -h 'bash -c ". venv/bin/activate && python ais_stream.py"'
	tmux split-window -v 'bash -c ". venv/bin/activate && python ais_pipeline/pipeline.py"'
	tmux attach-session -d