import argparse
import os
import sys
import uuid
from pathlib import Path

EXPECTED = {
    "Notebook": 11,
    "DataPipeline": 4,
    "SemanticModel": 1,
    "Report": 1,
}

def validate():
    root = Path(__file__).resolve().parents[1]
    definitions = root / "fabric" / "definitions"

    if not definitions.is_dir():
        raise RuntimeError("Fabric definitions directory missing.")

    workspace_id = os.getenv("FABRIC_WORKSPACE_ID", "")

    try:
        uuid.UUID(workspace_id)
    except (ValueError, AttributeError):
        raise RuntimeError("Invalid FABRIC_WORKSPACE_ID.")

    counts = {kind: 0 for kind in EXPECTED}

    for artifact in definitions.iterdir():
        if not artifact.is_dir():
            continue

        kind = artifact.name.rsplit(".", 1)[-1]

        if kind not in EXPECTED:
            raise RuntimeError(
                f"Unexpected artifact type: {artifact.name}"
            )

        if not (artifact / ".platform").is_file():
            raise RuntimeError(
                f"Missing .platform: {artifact.name}"
            )

        counts[kind] += 1

    for kind, expected in EXPECTED.items():
        print(f"{kind}: {counts[kind]}")

        if counts[kind] != expected:
            raise RuntimeError(
                f"Artifact count mismatch for {kind}."
            )

    print(f"Total artifacts: {sum(counts.values())}")
    print("PASS: Fabric definitions validated.")

    return definitions, workspace_id

def publish(definitions, workspace_id):
    from azure.identity import AzureCliCredential
    from fabric_cicd import FabricWorkspace, publish_all_items

    credential = AzureCliCredential()

    workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment="DEV",
        repository_directory=str(definitions),
        item_type_in_scope=list(EXPECTED.keys()),
        token_credential=credential,
    )

    publish_all_items(workspace)

    print("PASS: Fabric publishing completed.")
    print("Automatic deletion executed: No")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["validate", "publish"],
        required=True,
    )
    args = parser.parse_args()

    definitions, workspace_id = validate()

    if args.mode == "validate":
        print("Deployment executed: No")
        return

    publish(definitions, workspace_id)

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)