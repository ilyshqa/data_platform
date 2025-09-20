from __future__ import annotations
import argparse, json, os
from pathlib import Path
from typing import Any, Dict
import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape
from .models import Pipeline

def _env(templates_dir: str) -> Environment:
    return Environment(
        loader=FileSystemLoader(templates_dir),
        autoescape=select_autoescape(enabled_extensions=()),
        trim_blocks=True,
        lstrip_blocks=True,
    )

def generate_dag(yaml_path: str, out_path: str) -> str:
    with open(yaml_path, "r", encoding="utf-8") as f:
        raw: Dict[str, Any] = yaml.safe_load(f)
    pipeline = Pipeline.model_validate(raw)

    templates_dir = str(Path(__file__).parent / "templates")
    env = _env(templates_dir)
    tpl = env.get_template("dag.py.j2")

    # Prepare render context
    config = {
        "source": pipeline.source.model_dump(),
        "transforms": [t.model_dump() for t in pipeline.transforms],
        "target": pipeline.target.model_dump(),
        "quality_checks": [q.model_dump() for q in pipeline.quality_checks],
    }
    ctx = dict(
        dag_id=pipeline.pipeline_id,
        schedule=pipeline.schedule,
        retries=pipeline.retries,
        tags=list(pipeline.tags) or ["generated"],
        config_json=json.dumps(config, indent=2, ensure_ascii=False),
    )

    rendered = tpl.render(**ctx)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(rendered)
    return out_path

def main():
    ap = argparse.ArgumentParser(description="Generate Airflow DAG from YAML pipeline")
    ap.add_argument("yaml", help="Path to pipeline YAML")
    ap.add_argument("--out", required=True, help="Output .py path for the DAG")
    args = ap.parse_args()
    path = generate_dag(args.yaml, args.out)
    print(f"Wrote DAG to: {path}")

if __name__ == "__main__":
    main()
