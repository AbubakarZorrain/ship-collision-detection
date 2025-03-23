from flask import Blueprint, jsonify
from ais_pipeline.pipeline import run_pipeline

pipeline_bp = Blueprint("pipeline", __name__)

@pipeline_bp.route("/run-pipeline")
def run_pipeline_route():
    try:
        run_pipeline()
        return jsonify({"message": "Pipeline started successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
