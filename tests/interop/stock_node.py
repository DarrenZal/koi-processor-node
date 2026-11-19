"""Run a stock koi-net node for the interop gate.

    <.venv-koinet>/bin/python tests/interop/stock_node.py full ROOT
    <.venv-koinet>/bin/python tests/interop/stock_node.py partial ROOT

ROOT holds the node's config.yaml (written by run_gate.py), key, cache and log.
KOI_GATE_TEMPLATE_SRC names the `src` directory of koi-net-node-template at the pinned
commit (tests/interop/setup_stock_nodes.sh), and PRIV_KEY_PASSWORD the key password.

`full` runs the template unmodified, exactly as its README does (`python -m <package>`).
The template ships only a full node, so `partial` composes the same template handler
and config shape with koi-net's own PartialNode and PartialNodeConfig. Nothing in either
node is ours.
"""
from __future__ import annotations

import os
import runpy
import sys
from pathlib import Path

PKG = "koi_net_CHANGE_NAME_node"


def main(argv: list[str]) -> int:
    if len(argv) != 3 or argv[1] not in ("full", "partial"):
        print(__doc__, file=sys.stderr)
        return 2
    kind, root = argv[1], Path(argv[2]).resolve()
    if not (root / "config.yaml").is_file():
        print(f"stock_node: {root}/config.yaml is missing", file=sys.stderr)
        return 2
    src = os.environ.get("KOI_GATE_TEMPLATE_SRC")
    if not src or not (Path(src) / PKG / "core.py").is_file():
        print("stock_node: KOI_GATE_TEMPLATE_SRC must name the template's src directory", file=sys.stderr)
        return 2
    sys.path.insert(0, src)
    os.chdir(root)

    if kind == "full":
        sys.argv = [f"-m {PKG}", str(root)]
        runpy.run_module(PKG, run_name="__main__", alter_sys=True)
        return 0

    from rid_lib.types import KoiNetNode
    from koi_net.config import KoiNetConfig, NodeProvides, PartialNodeConfig, PartialNodeProfile
    from koi_net.core import PartialNode

    TemplateHandler = __import__(f"{PKG}.custom_handler", fromlist=["CustomHandler"]).CustomHandler

    class StockPartialConfig(PartialNodeConfig):
        # Mirrors the template's MyNodeConfig defaults, with a partial profile.
        koi_net: KoiNetConfig = KoiNetConfig(
            node_name="my-node-name",
            node_profile=PartialNodeProfile(provides=NodeProvides(event=[], state=[])),
            rid_types_of_interest=[KoiNetNode],
        )

    class StockPartialNode(PartialNode):
        config_schema = StockPartialConfig
        custom_handler = TemplateHandler

    StockPartialNode(root_dir=root).run()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
