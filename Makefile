.PHONY: setup ais_stream pipeline_live flask_app cleanup run_all

SHELL := /bin/bash

setup:
	@echo "flask app setup..."
	python3 -m venv venv
	. venv/bin/activate && pip install --upgrade pip
	. venv/bin/activate && pip install -r requirements.txt

ais_stream:
	@echo "ais_stream setup..."
	. venv/bin/activate && \
	python ais_stream.py

pipeline_live:
	@echo "Pipeline Dataflow (LIVE, collisions) ..."
	. venv/bin/activate && \
	export $(shell sed '/^ *#/d; /^$$/d' .env | xargs) && \
	echo "Pipeline running ..." && \
	python ais_pipeline/pipeline.py \
		--runner=DataflowRunner \
		--project=$$GOOGLE_CLOUD_PROJECT \
		--region=$$REGION \
		--staging_location=$$STAGING_LOCATION \
		--temp_location=$$TEMP_LOCATION \
		--job_name=$$JOB_NAME \
		--requirements_file=requirements.txt \
		--save_main_session \

flask_app:
	@echo "Run Flask..."
	. venv/bin/activate && \
	export $(shell sed '/^ *#/d; /^$$/d' .env | xargs) && \
	flask run

run_all:
	@echo "Starting all services in tmux..."
	tmux new-session -d -s ship-collision 'bash -c ". venv/bin/activate && make flask_app; exec bash"'
	tmux split-window -h 'bash -c ". venv/bin/activate && make ais_stream; exec bash"'
	tmux split-window -v 'bash -c ". venv/bin/activate && make pipeline_live; exec bash"'
	tmux attach-session -d