"""
Knowledge Graph Builder — assembles nodes + weighted edges from Firestore data.

Reads:
  - State-report API data (PLFS, SRS, HCES, UDISE, NCRB, ASI, CoL, SDG)
  - Manifesto promises (from Firestore)
  - Candidate/MLA data (from Firestore)
  - Constituency/district structure (from constituency map)

Applies:
  - bridge_rules.py (SDG→indicator, manifesto→SDG, inter-indicator influences)
  - ontology.json (node types, colors, allowed connections)

Outputs:
  - data/processed/knowledge_graph.json  (nodes + edges for react-force-graph)
  - Optional: upload to Firestore for API serving

Run:
    python scrapers/knowledge_graph/graph_builder.py
    python scrapers/knowledge_graph/graph_builder.py --upload
    python scrapers/knowledge_graph/graph_builder.py --stats

Output format (D3-compatible):
{
  "nodes": [{"id": "...", "type": "...", "label": "...", "layer": "...", ...}],
  "edges": [{"source": "...", "target": "...", "verb": "...", "weight": ..., ...}],
  "meta": {...}
}
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(Path(__file__).parent.parent))
from ts_utils import get_firestore_client

from bridge_rules import (
    SDG_TO_INDICATORS,
    MANIFESTO_CATEGORY_TO_SDG,
    INDICATOR_INFLUENCES,
    SDG_GOAL_NAMES,
)

OUT_PATH = BASE_DIR / "data" / "processed" / "knowledge_graph.json"
ONTOLOGY_PATH = Path(__file__).parent / "ontology.json"

FOCUS_STATES = {
    "tamil_nadu":       "Tamil Nadu",
    "kerala":           "Kerala",
    "karnataka":        "Karnataka",
    "andhra_pradesh":   "Andhra Pradesh",
    "telangana":        "Telangana",
}

# Short party ID → candidate party full name (for node ID matching)
# Used by manifesto parser + alliance edge builder
PARTY_ID_TO_FULL: dict[str, str] = {
    "aiadmk": "All India Anna Dravida Munnetra Kazhagam",
    "dmk":    "Dravida Munnetra Kazhagam",
    "ntk":    "Naam Tamilar Katchi",
    "tvk":    "Tamilaga Vettri Kazhagam",
    "bjp":    "Bharatiya Janata Party",
    "inc":    "Indian National Congress",
    "cpi":    "Communist Party of India",
    "cpim":   "Communist Party of India (Marxist)",
    "pmk":    "Pattali Makkal Katchi",
    "dmdk":   "Desiya Murpokku Dravida Kazhagam",
    "vck":    "Viduthalai Chiruthaigal Katchi",
    "mdmk":   "Marumalarchi Dravida Munnetra Kazhagam",
    "mmk":    "Manithaneya Makkal Katchi",
    "iuml":   "Indian Union Muslim League",
    "tmc":    "Tamil Maanila Congress (Moopanar)",
}

# Constituency map for district→constituency structure
CONSTITUENCY_MAP_PATH = BASE_DIR / "web" / "src" / "lib" / "constituency-map.json"


class GraphBuilder:
    def __init__(self, db):
        self.db = db
        self.nodes: list[dict] = []
        self.edges: list[dict] = []
        self._node_ids: set[str] = set()
        self.ontology = json.loads(ONTOLOGY_PATH.read_text())

    def _add_node(self, node_id: str, node_type: str, label: str, **kwargs) -> None:
        if node_id in self._node_ids:
            return
        self._node_ids.add(node_id)
        type_def = self.ontology["node_types"].get(node_type, {})
        self.nodes.append({
            "id": node_id,
            "type": node_type,
            "label": label,
            "layer": type_def.get("layer", "unknown"),
            "color": type_def.get("color", "#999"),
            **kwargs,
        })

    def _add_edge(self, source: str, target: str, verb: str, weight: float = 1.0, **kwargs) -> None:
        if source not in self._node_ids or target not in self._node_ids:
            return
        self.edges.append({
            "source": source,
            "target": target,
            "verb": verb,
            "weight": weight,
            **kwargs,
        })

    # ── Layer 1: Foundation (States, Districts, Constituencies) ──

    def build_foundation(self) -> None:
        print("  Building foundation layer...")

        # States
        for slug, name in FOCUS_STATES.items():
            self._add_node(f"state:{slug}", "state", name, slug=slug)

        # Districts + constituencies from map
        if CONSTITUENCY_MAP_PATH.exists():
            cmap = json.loads(CONSTITUENCY_MAP_PATH.read_text())
            districts_seen: set[str] = set()

            for ac_slug, entry in cmap.items():
                ac_name = entry.get("name", ac_slug.replace("_", " ").title())
                district = entry.get("district", "")
                ac_type = entry.get("type", "GEN")

                if not district:
                    continue

                # District node
                dist_slug = district.lower().replace(" ", "_")
                dist_id = f"district:{dist_slug}"
                if dist_id not in districts_seen:
                    districts_seen.add(dist_id)
                    self._add_node(dist_id, "district", district, slug=dist_slug)
                    self._add_edge("state:tamil_nadu", dist_id, "contains")

                # Constituency node
                ac_id = f"constituency:{ac_slug}"
                self._add_node(ac_id, "constituency", ac_name, slug=ac_slug, ac_type=ac_type)
                self._add_edge(dist_id, ac_id, "contains")

        count = sum(1 for n in self.nodes if n["type"] in ("state", "district", "constituency"))
        print(f"    {count} foundation nodes")

    # ── Layer 4: Political (Parties, Candidates, MLAs, Manifestos) ──

    def build_political(self) -> None:
        print("  Building political layer...")

        # Pass 1: collect parties and create party nodes FIRST
        parties: dict[str, int] = {}
        candidate_rows: list[tuple[str, dict]] = []  # (constituency_slug, candidate_dict)
        try:
            docs = list(self.db.collection("candidates_2026").stream())
            for doc in docs:
                data = doc.to_dict()
                for c in data.get("candidates", []):
                    party = c.get("party", "")
                    if party:
                        parties[party] = parties.get(party, 0) + 1
                    candidate_rows.append((doc.id, c))
        except Exception as e:
            print(f"    WARNING: Could not read candidates_2026: {e}")

        for party_name, count in parties.items():
            party_id = f"party:{party_name.lower().replace(' ', '_')}"
            self._add_node(party_id, "party", party_name, candidate_count=count)

        # Pass 2: create candidate nodes and edges (parties already exist)
        for cslug, c in candidate_rows:
            cname = c.get("name", "")
            party = c.get("party", "")
            if cname and party:
                cand_id = f"candidate:{cslug}:{cname.lower().replace(' ', '_')[:30]}"
                self._add_node(cand_id, "candidate", cname,
                               party=party, constituency=cslug)
                party_id = f"party:{party.lower().replace(' ', '_')}"
                ac_id = f"constituency:{cslug}"
                self._add_edge(cand_id, party_id, "belongs_to")
                self._add_edge(cand_id, ac_id, "contests")

        # MLA data — from candidate_accountability (2021 winners)
        try:
            docs = list(self.db.collection("candidate_accountability").stream())
            for doc in docs:
                data = doc.to_dict()
                name = data.get("name", data.get("candidate_name", ""))
                party = data.get("party", "")
                ac_slug = data.get("constituency", doc.id).lower().replace(" ", "_")
                assets = data.get("assets_cr")
                cases = data.get("criminal_cases", 0)

                if name:
                    mla_id = f"mla:{ac_slug}"
                    self._add_node(mla_id, "mla", name,
                                   party=party, constituency=ac_slug,
                                   assets=assets, criminal_cases=cases)
                    ac_id = f"constituency:{ac_slug}"
                    self._add_edge(mla_id, ac_id, "represents")
                    if party:
                        party_id = f"party:{party.lower().replace(' ', '_')}"
                        self._add_edge(mla_id, party_id, "belongs_to")
        except Exception as e:
            print(f"    WARNING: Could not read MLAs: {e}")

        # Manifesto promises — each doc is one promise (flat structure)
        try:
            docs = list(self.db.collection("manifesto_promises").stream())
            for doc in docs:
                data = doc.to_dict()
                party_name = data.get("party_name", "")
                raw_party_id = data.get("party_id", "")
                # Map short manifesto party_id to full candidate party name for node matching
                full_name = PARTY_ID_TO_FULL.get(raw_party_id, party_name)
                party_id = f"party:{full_name.lower().replace(' ', '_')}"
                text = (data.get("promise_text_en") or data.get("scheme_name") or "")[:80]
                category = data.get("category", "")
                status = data.get("status", "")

                if not text:
                    continue

                promise_id = f"promise:{doc.id}"
                self._add_node(promise_id, "manifesto_item", text,
                               party=party_name, category=category, status=status)
                self._add_edge(party_id, promise_id, "promised")

                # Link to SDG goals via category
                cat_key = category.lower().strip().replace("'", "").replace("&", "and")
                cat_key = "_".join(cat_key.split())  # normalize whitespace to single _
                # Also try common aliases
                CAT_ALIASES = {
                    "agriculture": "agriculture",
                    "education": "education",
                    "infrastructure": "infrastructure",
                    "women's_welfare": "women_empowerment",
                    "womens_welfare": "women_empowerment",
                    "tasmac_and_revenue": "poverty_alleviation",
                    "healthcare": "healthcare",
                    "health": "healthcare",
                    "employment": "employment",
                    "industry": "industry",
                    "law_and_order": "law_and_order",
                    "social_justice": "social_justice",
                    "housing": "housing",
                    "energy": "energy",
                    "environment": "environment",
                    "water_sanitation": "water_sanitation",
                }
                cat_key = CAT_ALIASES.get(cat_key, cat_key)
                cat_mappings = MANIFESTO_CATEGORY_TO_SDG.get(cat_key, [])
                for mapping in cat_mappings:
                    sdg_id = f"sdg:{mapping['sdg']}"
                    self._add_edge(promise_id, sdg_id, "targets_goal",
                                   weight=mapping["weight"])

        except Exception as e:
            print(f"    WARNING: Could not read manifesto: {e}")

        pol_count = sum(1 for n in self.nodes if n["layer"] == "political")
        print(f"    {pol_count} political nodes")

    # ── Alliance edges (party ↔ party, temporal) ──

    def build_alliances(self) -> None:
        print("  Building alliance edges...")
        count = 0
        try:
            docs = list(self.db.collection("alliances").stream())
            for doc in docs:
                data = doc.to_dict()
                year = data.get("year")
                members = data.get("member_parties", [])
                anchor = data.get("anchor_party", "")
                alliance_name = data.get("alliance_name", "")

                if not members or not year:
                    continue

                # Create edges between all alliance members (anchor → each member)
                for member in members:
                    if member == anchor:
                        continue
                    # Need to find the party node ID — try matching short IDs
                    anchor_id = self._find_party_node(anchor)
                    member_id = self._find_party_node(member)
                    if anchor_id and member_id:
                        self._add_edge(anchor_id, member_id, "allied_with",
                                       weight=0.8, period=year,
                                       alliance=alliance_name)
                        count += 1
        except Exception as e:
            print(f"    WARNING: Could not read alliances: {e}")
        print(f"    {count} alliance edges")

    # ── Won-seat edges (party → constituency, temporal) ──

    def build_won_seats(self) -> None:
        print("  Building won-seat edges...")
        count = 0
        try:
            docs = list(self.db.collection("candidate_accountability").stream())
            for doc in docs:
                data = doc.to_dict()
                party = data.get("party", "")
                constituency = data.get("constituency", "")
                # Doc ID format: {year}_{ac_slug}
                parts = doc.id.split("_", 1)
                year = int(parts[0]) if parts[0].isdigit() else None

                if not party or not constituency or not year:
                    continue

                # Normalize constituency to slug — handle mixed case + spaces
                ac_slug = parts[1] if len(parts) > 1 else constituency.lower().replace(" ", "_")
                party_id = self._find_party_node_by_name(party)
                ac_id = f"constituency:{ac_slug}"

                if party_id and ac_id in self._node_ids:
                    self._add_edge(party_id, ac_id, "won",
                                   weight=1.0, period=year)
                    count += 1
        except Exception as e:
            print(f"    WARNING: Could not read won seats: {e}")
        print(f"    {count} won-seat edges")

    # ── Operates-in edges (party → state) ──

    def build_operates_in(self) -> None:
        """Link every party node to state:tamil_nadu."""
        print("  Building operates-in edges...")
        count = 0
        state_id = "state:tamil_nadu"
        for node in self.nodes:
            if node["type"] == "party":
                self._add_edge(node["id"], state_id, "operates_in", weight=0.3)
                count += 1
        print(f"    {count} operates-in edges")

    # ── Party node lookup helpers ──

    def _find_party_node(self, short_id: str) -> str | None:
        """Find party node ID by short alliance ID (e.g., 'dmk', 'bjp')."""
        short_lower = short_id.lower().strip()
        # Try known mappings first
        full_name = PARTY_ID_TO_FULL.get(short_lower)
        if full_name:
            node_id = f"party:{full_name.lower().replace(' ', '_')}"
            if node_id in self._node_ids:
                return node_id

        # Fuzzy: check if any party node ID contains the short ID
        for nid in self._node_ids:
            if nid.startswith("party:") and short_lower in nid:
                return nid
        return None

    def _find_party_node_by_name(self, name: str) -> str | None:
        """Find party node ID by full name, abbreviation, or partial match."""
        # Direct full-name match
        node_id = f"party:{name.lower().replace(' ', '_')}"
        if node_id in self._node_ids:
            return node_id
        # Try PARTY_ID_TO_FULL mapping (handles short IDs like DMK, AIADMK)
        full = PARTY_ID_TO_FULL.get(name.lower())
        if full:
            node_id = f"party:{full.lower().replace(' ', '_')}"
            if node_id in self._node_ids:
                return node_id
        # Common abbreviation → full name for candidate_accountability data
        _ABBREV = {
            "CPM": "Communist Party of India (Marxist)",
            "CPI(M)": "Communist Party of India (Marxist)",
            "IND": "Independent",
            "AIFB": "All India Forward Bloc",
        }
        mapped = _ABBREV.get(name)
        if mapped:
            node_id = f"party:{mapped.lower().replace(' ', '_')}"
            if node_id in self._node_ids:
                return node_id
        # Partial match
        slug = name.lower().replace(" ", "_")
        for nid in self._node_ids:
            if nid.startswith("party:") and slug in nid:
                return nid
        return None

    # ── Layer 2: Bridge (SDG Goals) ──

    def _create_sdg_nodes(self) -> None:
        """Create SDG goal nodes (called early so targets_goal edges can resolve)."""
        for goal_num, goal_name in SDG_GOAL_NAMES.items():
            sdg_id = f"sdg:{goal_num}"
            self._add_node(sdg_id, "sdg_goal", f"SDG {goal_num}: {goal_name}",
                           goal_number=int(goal_num), goal_name=goal_name)
        print(f"  Created {len(SDG_GOAL_NAMES)} SDG goal nodes")

    def build_sdg_bridge(self) -> None:
        print("  Building SDG bridge edges...")

        # SDG → Indicator edges (from bridge rules)
        for goal_num, indicators in SDG_TO_INDICATORS.items():
            sdg_id = f"sdg:{goal_num}"
            for ind in indicators:
                # Link to each state's indicator node
                for state_slug in FOCUS_STATES:
                    ind_id = f"{ind['indicator']}:{state_slug}"
                    self._add_edge(sdg_id, ind_id, "measured_by",
                                   weight=ind["weight"],
                                   field=ind["field"],
                                   reason=ind["reason"])

        print(f"    {len(SDG_GOAL_NAMES)} SDG goal nodes")

    # ── Layer 3: Socioeconomic Indicators ──

    def build_indicators(self) -> None:
        print("  Building indicator layer...")

        KG_COLLECTIONS = ["plfs", "srs", "hces", "udise", "ncrb", "asi"]
        indicator_map = {
            "plfs":  "indicator_plfs",
            "srs":   "indicator_srs",
            "hces":  "indicator_hces",
            "udise": "indicator_udise",
            "ncrb":  "indicator_ncrb",
            "asi":   "indicator_asi",
        }

        for state_slug, state_name in FOCUS_STATES.items():
            state_id = f"state:{state_slug}"

            for col in KG_COLLECTIONS:
                ind_type = indicator_map[col]
                ind_id = f"{ind_type}:{state_slug}"

                # Fetch latest snapshot
                snap = self._latest_snapshot(col, state_slug)
                if not snap:
                    continue

                period = snap.pop("period", snap.pop("data_period", ""))
                label = f"{self.ontology['node_types'][ind_type]['label']} — {state_name}"

                self._add_node(ind_id, ind_type, label, period=period, state=state_slug, **snap)
                self._add_edge(state_id, ind_id, "describes")

            # Cost of Living
            col_snap = self._latest_snapshot("cost_of_living", f"cost_of_living_{state_slug}")
            if col_snap:
                period = col_snap.pop("period", col_snap.pop("data_period", ""))
                col_id = f"indicator_col:{state_slug}"
                label = f"Cost of Living — {state_name}"
                self._add_node(col_id, "indicator_col", label, period=period, state=state_slug)
                self._add_edge(state_id, col_id, "describes")

            # AISHE
            aishe_snap = self._latest_snapshot("aishe", state_slug)
            if aishe_snap:
                period = aishe_snap.pop("period", aishe_snap.pop("data_period", ""))
                aishe_id = f"indicator_aishe:{state_slug}"
                label = f"Higher Education (AISHE) — {state_name}"
                self._add_node(aishe_id, "indicator_aishe", label, period=period, state=state_slug)
                self._add_edge(state_id, aishe_id, "describes")

        count = sum(1 for n in self.nodes if n["layer"] == "socioeconomic")
        print(f"    {count} indicator nodes")

    # ── Inter-indicator causal links ──

    def build_causal_links(self) -> None:
        print("  Building inter-indicator causal links...")
        count = 0
        for influence in INDICATOR_INFLUENCES:
            for state_slug in FOCUS_STATES:
                src = f"{influence['source']}:{state_slug}"
                tgt = f"{influence['target']}:{state_slug}"
                if src in self._node_ids and tgt in self._node_ids:
                    self._add_edge(src, tgt, "influences",
                                   weight=influence["weight"],
                                   direction=influence["direction"],
                                   reason=influence["reason"])
                    count += 1
        print(f"    {count} causal edges")

    # ── Helpers ──

    def _latest_snapshot(self, collection: str, entity_slug: str) -> dict | None:
        try:
            snaps = list(
                self.db.collection(collection)
                .document(entity_slug)
                .collection("snapshots")
                .stream()
            )
            if not snaps:
                return None
            latest = max(snaps, key=lambda d: d.id)
            return {"period": latest.id, **(latest.to_dict() or {})}
        except Exception:
            return None

    # ── Build all ──

    def build(self) -> dict:
        self.build_foundation()
        self._create_sdg_nodes()       # SDG nodes first (targets_goal needs them)
        self.build_indicators()        # indicators before SDG edges (measured_by needs them)
        self.build_political()         # parties+candidates+manifesto (promised, targets_goal)
        self.build_alliances()         # party↔party alliance edges (temporal)
        self.build_won_seats()         # party→constituency won edges (temporal)
        self.build_operates_in()       # party→state structural anchoring
        self.build_sdg_bridge()        # measured_by edges (indicators + SDG nodes both exist now)
        self.build_causal_links()

        return {
            "nodes": self.nodes,
            "edges": self.edges,
            "meta": {
                "node_count": len(self.nodes),
                "edge_count": len(self.edges),
                "layers": list(self.ontology["layers"].keys()),
                "states": list(FOCUS_STATES.values()),
                "node_types": {
                    t: sum(1 for n in self.nodes if n["type"] == t)
                    for t in self.ontology["node_types"]
                },
                "edge_verbs": {
                    v: sum(1 for e in self.edges if e["verb"] == v)
                    for v in self.ontology["edge_verbs"]
                },
            },
        }


def precompute_layout(graph: dict) -> dict:
    """
    Run a spring layout using networkx to compute stable x/y positions.
    Injects fx/fy (fixed position) into each node so the frontend
    renders deterministically without simulation jitter.
    """
    import networkx as nx

    print("  Pre-computing layout with networkx spring_layout...")

    G = nx.Graph()
    for n in graph["nodes"]:
        G.add_node(n["id"])
    for e in graph["edges"]:
        src = e["source"] if isinstance(e["source"], str) else e["source"]["id"]
        tgt = e["target"] if isinstance(e["target"], str) else e["target"]["id"]
        w = e.get("weight", 1.0)
        G.add_edge(src, tgt, weight=w)

    # Spring layout — k controls spacing, iterations controls convergence
    # Higher k = more spread. Scale up to pixel coordinates.
    pos = nx.spring_layout(G, k=2.5, iterations=150, seed=42, scale=800)

    # Inject fx/fy into nodes (react-force-graph uses fx/fy for fixed positions)
    pos_map = {nid: (float(xy[0]), float(xy[1])) for nid, xy in pos.items()}
    for node in graph["nodes"]:
        xy = pos_map.get(node["id"])
        if xy:
            node["fx"] = round(xy[0], 1)
            node["fy"] = round(xy[1], 1)

    print(f"    Positioned {len(pos_map)} nodes")
    return graph


def main():
    upload = "--upload" in sys.argv
    stats  = "--stats"  in sys.argv
    no_layout = "--no-layout" in sys.argv

    print("Building knowledge graph...")
    db = get_firestore_client()
    builder = GraphBuilder(db)
    graph = builder.build()

    print(f"\n  Nodes: {graph['meta']['node_count']}")
    print(f"  Edges: {graph['meta']['edge_count']}")

    if stats:
        print("\n  Node types:")
        for t, c in sorted(graph["meta"]["node_types"].items(), key=lambda x: -x[1]):
            if c > 0:
                print(f"    {t:<25} {c:>5}")
        print("\n  Edge verbs:")
        for v, c in sorted(graph["meta"]["edge_verbs"].items(), key=lambda x: -x[1]):
            if c > 0:
                print(f"    {v:<25} {c:>5}")
        return

    # Pre-compute deterministic layout
    if not no_layout:
        graph = precompute_layout(graph)

    # Save full graph locally
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(graph, default=str, ensure_ascii=False))
    print(f"\n  Wrote {OUT_PATH}  ({OUT_PATH.stat().st_size // 1024} KB)")

    # Save slim version for API/frontend (smaller payload)
    SLIM_FIELDS = {"id", "type", "label", "layer", "color", "party", "constituency",
                   "slug", "category", "status", "goal_number", "goal_name",
                   "state", "period", "candidate_count", "assets", "criminal_cases",
                   "fx", "fy"}
    slim_nodes = [{k: v for k, v in n.items() if k in SLIM_FIELDS} for n in graph["nodes"]]
    slim_edges = [{"source": e["source"], "target": e["target"], "verb": e["verb"],
                   "weight": e.get("weight", 1.0),
                   **({"period": e["period"]} if "period" in e else {})}
                  for e in graph["edges"]]
    slim_graph = {"nodes": slim_nodes, "edges": slim_edges, "meta": graph["meta"]}
    slim_path = OUT_PATH.with_name("knowledge_graph_slim.json")
    slim_path.write_text(json.dumps(slim_graph, default=str, ensure_ascii=False))
    print(f"  Wrote {slim_path}  ({slim_path.stat().st_size // 1024} KB)")

    if upload:
        print("  Uploading to Firestore (knowledge_graph/latest)...")
        db.collection("knowledge_graph").document("latest").set(graph)
        print("  Done.")


if __name__ == "__main__":
    main()
