"use client";

import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import dynamic from "next/dynamic";
import Link from "next/link";

// Force graph components — client-only (use canvas/WebGL + window)
const ForceGraph2D = dynamic(() => import("react-force-graph-2d"), {
  ssr: false,
});
const ForceGraph3D = dynamic(() => import("react-force-graph-3d"), {
  ssr: false,
});

import { apiGet } from "@/lib/api-client";


// ─── Types ──────────────────────────────────────────────────────────────────

interface GraphNode {
  id: string;
  type: string;
  label: string;
  layer: string;
  color: string;
  party?: string;
  constituency?: string;
  category?: string;
  status?: string;
  goal_number?: number;
  goal_name?: string;
  candidate_count?: number;
  assets?: number;
  criminal_cases?: number;
  state?: string;
  period?: string;
  // Pre-computed layout positions (fixed)
  fx?: number;
  fy?: number;
  // Force graph adds these at runtime
  x?: number;
  y?: number;
}

interface GraphEdge {
  source: string | GraphNode;
  target: string | GraphNode;
  verb: string;
  weight: number;
  period?: number | string;
}

interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
  meta: {
    node_count: number;
    edge_count: number;
    node_types: Record<string, number>;
    edge_verbs: Record<string, number>;
  };
}

// ─── Layer / type config ────────────────────────────────────────────────────

const LAYER_CONFIG: Record<string, { label: string; order: number }> = {
  foundation: { label: "Foundation", order: 0 },
  socioeconomic: { label: "Social Pulse", order: 1 },
  bridge: { label: "SDG Bridge", order: 2 },
  political: { label: "Political", order: 3 },
};

const NODE_SIZE: Record<string, number> = {
  state: 8,
  district: 3,
  constituency: 2,
  party: 5,
  candidate: 1.5,
  mla: 3,
  manifesto_item: 2,
  sdg_goal: 5,
  indicator_plfs: 4,
  indicator_srs: 4,
  indicator_hces: 4,
  indicator_udise: 4,
  indicator_aishe: 4,
  indicator_ncrb: 4,
  indicator_asi: 4,
  indicator_col: 4,
  indicator_fiscal: 5,
};

// Link distance by verb — spreads clusters apart
const LINK_DISTANCE: Record<string, number> = {
  contains: 40,
  belongs_to: 30,
  contests: 25,
  represents: 35,
  promised: 50,
  targets_goal: 70,
  measured_by: 80,
  describes: 60,
  influences: 90,
  allied_with: 45,
  won: 35,
  operates_in: 100,
};

// Human-readable display names for the legend
const NODE_TYPE_LABELS: Record<string, string> = {
  candidate: "Candidate",
  manifesto_item: "Manifesto Promise",
  constituency: "Constituency",
  party: "Political Party",
  district: "District",
  sdg_goal: "SDG Goal",
  state: "State",
  mla: "Sitting MLA",
  indicator_plfs: "Labour (PLFS)",
  indicator_srs: "Health (SRS)",
  indicator_hces: "Spending (HCES)",
  indicator_udise: "School Education (UDISE+)",
  indicator_aishe: "Higher Education (AISHE)",
  indicator_ncrb: "Crime (NCRB)",
  indicator_asi: "Industry (ASI)",
  indicator_col: "Cost of Living",
  indicator_fiscal: "State Finances (CAG)",
};

// ─── Temporal config ────────────────────────────────────────────────────────

// Key time markers for the slider
const TIME_MARKERS = [
  { year: 2006, label: "2006 Election" },
  { year: 2011, label: "2011 Election" },
  { year: 2016, label: "2016 Election" },
  { year: 2021, label: "2021 Election" },
  { year: 2026, label: "2026 Election" },
];

const MIN_YEAR = 2006;
const MAX_YEAR = 2026;

/**
 * Normalize a period string or number to a numeric year for comparison.
 * "2023-24" → 2023, "2021-22" → 2021, 2021 → 2021, "2026-04" → 2026
 */
function periodToYear(p: string | number | undefined | null): number | null {
  if (p == null) return null;
  const s = String(p);
  const m = s.match(/^(\d{4})/);
  return m ? parseInt(m[1]) : null;
}

// Label visibility threshold per node type (zoom level)
const LABEL_ZOOM_THRESHOLD: Record<string, number> = {
  state: 0.3,
  sdg_goal: 0.5,
  party: 0.6,
  indicator_plfs: 0.8,
  indicator_srs: 0.8,
  indicator_hces: 0.8,
  indicator_udise: 0.8,
  indicator_aishe: 0.8,
  indicator_ncrb: 0.8,
  indicator_asi: 0.8,
  indicator_col: 0.8,
  indicator_fiscal: 0.8,
  district: 1.5,
  mla: 2.0,
  manifesto_item: 2.5,
  constituency: 3.0,
  candidate: 5.0,
};

const VERB_COLORS: Record<string, string> = {
  contains: "#94a3b8",
  belongs_to: "#94a3b8",
  contests: "#60a5fa",
  represents: "#f59e0b",
  promised: "#a855f7",
  targets_goal: "#22c55e",
  measured_by: "#14b8a6",
  describes: "#6b7280",
  influences: "#ef4444",
  allied_with: "#f97316",
  won: "#facc15",
  operates_in: "#64748b",
};

// ─── Component ──────────────────────────────────────────────────────────────

export default function KnowledgeGraphPage() {
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null);
  const [hiddenTypes, setHiddenTypes] = useState<Set<string>>(
    new Set(["candidate"]) // hide candidates by default (4488 nodes)
  );
  const [searchQuery, setSearchQuery] = useState("");
  const [legendCollapsed, setLegendCollapsed] = useState(false);
  const [view3D, setView3D] = useState(true);
  const [timeRange, setTimeRange] = useState<number>(MAX_YEAR); // show edges up to this year
  const hasZoomedToFit = useRef(false);
  const [highlightNodes, setHighlightNodes] = useState<Set<string>>(new Set());
  const [highlightEdges, setHighlightEdges] = useState<Set<string>>(new Set());
  const [mounted, setMounted] = useState(false);
  const [zoomLevel, setZoomLevel] = useState(1);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const graphRef = useRef<any>(null);

  // Avoid hydration mismatch — ForceGraph2D needs window/canvas
  useEffect(() => { setMounted(true); }, []);

  // Prevent browser zoom (Ctrl+scroll, pinch) on this page — let the graph canvas handle it
  useEffect(() => {
    const prevent = (e: WheelEvent) => {
      if (e.ctrlKey || e.metaKey) e.preventDefault();
    };
    const preventTouch = (e: TouchEvent) => {
      if (e.touches.length > 1) e.preventDefault();
    };
    document.addEventListener("wheel", prevent, { passive: false });
    document.addEventListener("touchmove", preventTouch, { passive: false });
    return () => {
      document.removeEventListener("wheel", prevent);
      document.removeEventListener("touchmove", preventTouch);
    };
  }, []);

  // Fetch graph data from backend API
  useEffect(() => {
    apiGet<GraphData>("/api/knowledge-graph")
      .then((data) => {
        setGraphData(data);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message || "Failed to load graph");
        setLoading(false);
      });
  }, []);

  // Filtered graph based on hidden types
  const filteredGraph = useMemo(() => {
    if (!graphData) return { nodes: [], links: [] };
    const visibleNodes = graphData.nodes.filter(
      (n) => !hiddenTypes.has(n.type)
    );
    const visibleIds = new Set(visibleNodes.map((n) => n.id));
    const visibleEdges = graphData.edges.filter((e) => {
      const src = typeof e.source === "string" ? e.source : e.source.id;
      const tgt = typeof e.target === "string" ? e.target : e.target.id;
      if (!visibleIds.has(src) || !visibleIds.has(tgt)) return false;
      // Temporal filter: edges with a period must be <= timeRange
      // Edges without period (structural) always pass
      const edgeYear = periodToYear(e.period);
      if (edgeYear != null && edgeYear > timeRange) return false;
      return true;
    });
    return { nodes: visibleNodes, links: visibleEdges };
  }, [graphData, hiddenTypes, timeRange]);

  // Pre-baked layout: nodes have fx/fy from the graph builder.
  // Only run a brief simulation to settle any floating nodes, then stop.
  useEffect(() => {
    if (!graphRef.current) return;
    const fg = graphRef.current;
    // Light forces for any nodes without fx/fy
    fg.d3Force("charge")?.strength(-50);
    fg.d3Force("center")?.strength(0.02);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    fg.d3Force("link")?.distance((link: any) => LINK_DISTANCE[link.verb] || 40);
    // 3D: increase zoom sensitivity via Three.js controls
    if (view3D && fg.controls) {
      const ctrl = fg.controls();
      if (ctrl && ctrl.zoomSpeed !== undefined) ctrl.zoomSpeed = 1.5;
    }
  }, [filteredGraph, view3D]);

  // Search
  useEffect(() => {
    if (!searchQuery.trim() || !graphData) {
      setHighlightNodes(new Set());
      return;
    }
    const q = searchQuery.toLowerCase();
    const matched = new Set(
      graphData.nodes
        .filter(
          (n) =>
            n.label.toLowerCase().includes(q) ||
            (n.party && n.party.toLowerCase().includes(q)) ||
            (n.category && n.category.toLowerCase().includes(q))
        )
        .map((n) => n.id)
    );
    setHighlightNodes(matched);
  }, [searchQuery, graphData]);

  // Node click → highlight neighbors
  const handleNodeClick = useCallback(
    (node: GraphNode) => {
      setSelectedNode(node);
      if (!graphData) return;

      const neighborIds = new Set<string>();
      const edgeKeys = new Set<string>();
      neighborIds.add(node.id);

      for (const e of graphData.edges) {
        const src = typeof e.source === "string" ? e.source : e.source.id;
        const tgt = typeof e.target === "string" ? e.target : e.target.id;
        if (src === node.id || tgt === node.id) {
          neighborIds.add(src);
          neighborIds.add(tgt);
          edgeKeys.add(`${src}→${tgt}`);
        }
      }
      setHighlightNodes(neighborIds);
      setHighlightEdges(edgeKeys);
    },
    [graphData]
  );

  const handleBackgroundClick = useCallback(() => {
    setSelectedNode(null);
    setHighlightNodes(new Set());
    setHighlightEdges(new Set());
  }, []);

  // Toggle node type visibility
  const toggleType = (type: string) => {
    setHiddenTypes((prev) => {
      const next = new Set(prev);
      if (next.has(type)) next.delete(type);
      else next.add(type);
      return next;
    });
  };

  // Node paint — semantic zoom: labels appear progressively as you zoom in
  // Node sizes scale inversely with zoom so they stay visually consistent
  const paintNode = useCallback(
    (node: GraphNode, ctx: CanvasRenderingContext2D) => {
      const baseSize = (NODE_SIZE[node.type] || 3) / Math.max(zoomLevel, 0.3);
      const x = node.x || 0;
      const y = node.y || 0;
      const isHighlighted =
        highlightNodes.size === 0 || highlightNodes.has(node.id);
      const alpha = isHighlighted ? 1 : 0.12;
      const hexAlpha = alpha < 1 ? Math.round(alpha * 255).toString(16).padStart(2, "0") : "";

      // Glow for highlighted important nodes
      const rawSize = NODE_SIZE[node.type] || 3;
      if (isHighlighted && highlightNodes.size > 0 && rawSize >= 4) {
        ctx.beginPath();
        ctx.arc(x, y, baseSize + 4 / Math.max(zoomLevel, 0.3), 0, 2 * Math.PI);
        ctx.fillStyle = node.color + "30";
        ctx.fill();
      }

      // Node circle
      ctx.beginPath();
      ctx.arc(x, y, baseSize, 0, 2 * Math.PI);
      ctx.fillStyle = node.color + hexAlpha;
      ctx.fill();

      // Border for indicator/SDG/state/party nodes
      if (rawSize >= 4) {
        ctx.strokeStyle = isHighlighted ? "#ffffff40" : "#ffffff10";
        ctx.lineWidth = 1;
        ctx.stroke();
      }

      // Semantic zoom labels — show based on zoom level and node type
      // Font size stays constant in screen pixels (divide by zoom to counteract canvas scaling)
      // Labels are offset in different directions based on node ID hash to avoid overlap
      const threshold = LABEL_ZOOM_THRESHOLD[node.type] ?? 3;
      if (zoomLevel >= threshold && isHighlighted) {
        const fontSize = 10 / zoomLevel;
        ctx.font = `${fontSize}px Inter, system-ui, sans-serif`;

        // Hash node ID to pick a label direction (8 directions)
        let hash = 0;
        for (let i = 0; i < node.id.length; i++) hash = ((hash << 5) - hash + node.id.charCodeAt(i)) | 0;
        const dir = ((hash % 8) + 8) % 8;
        const offset = baseSize + fontSize * 0.5;
        const angles = [0, Math.PI / 4, Math.PI / 2, (3 * Math.PI) / 4, Math.PI, (5 * Math.PI) / 4, (3 * Math.PI) / 2, (7 * Math.PI) / 4];
        const angle = angles[dir];
        const lx = x + Math.cos(angle) * offset;
        const ly = y + Math.sin(angle) * offset;

        // Align text based on direction
        const cos = Math.cos(angle);
        ctx.textAlign = cos > 0.3 ? "left" : cos < -0.3 ? "right" : "center";
        ctx.textBaseline = Math.sin(angle) > 0.3 ? "top" : Math.sin(angle) < -0.3 ? "bottom" : "middle";
        ctx.fillStyle = isHighlighted ? "#e5e7eb" : "#6b728060";
        const maxLen = zoomLevel > 3 ? 40 : 20;
        ctx.fillText(node.label.slice(0, maxLen), lx, ly);
      }
    },
    [highlightNodes, zoomLevel]
  );

  // Edge paint — bridge edges are thicker and more visible
  const paintLink = useCallback(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (link: any, ctx: CanvasRenderingContext2D) => {
      const src = link.source;
      const tgt = link.target;
      if (!src?.x || !tgt?.x) return;

      const edgeKey = `${src.id}→${tgt.id}`;
      const hasSelection = highlightEdges.size > 0;
      const isHighlighted = !hasSelection || highlightEdges.has(edgeKey);
      const verb: string = link.verb || "";

      // Base width by edge importance
      const isBridge = ["targets_goal", "measured_by", "influences", "promised"].includes(verb);
      const baseWidth = isBridge ? 1.2 : 0.4;

      const zScale = 1 / Math.max(zoomLevel, 0.3);

      if (!isHighlighted) {
        ctx.beginPath();
        ctx.moveTo(src.x, src.y);
        ctx.lineTo(tgt.x, tgt.y);
        ctx.strokeStyle = "#ffffff06";
        ctx.lineWidth = 0.15 * zScale;
        ctx.stroke();
        return;
      }

      ctx.beginPath();
      ctx.moveTo(src.x, src.y);
      ctx.lineTo(tgt.x, tgt.y);
      ctx.strokeStyle = VERB_COLORS[verb] || "#6b7280";
      ctx.lineWidth = (hasSelection ? baseWidth * 2.5 : baseWidth) * zScale;
      ctx.globalAlpha = hasSelection ? 1 : (isBridge ? 0.6 : 0.25);
      ctx.stroke();
      ctx.globalAlpha = 1;
    },
    [highlightEdges, zoomLevel]
  );

  // ─── Render ──────────────────────────────────────────────────────────────

  if (!mounted || loading) {
    return (
      <div className="min-h-screen bg-gray-950 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full mx-auto mb-4" />
          <p className="text-gray-400 text-sm">Loading knowledge graph...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-950 flex items-center justify-center">
        <p className="text-red-400">Failed to load graph: {error}</p>
      </div>
    );
  }

  const nodeTypes = graphData
    ? Object.entries(graphData.meta.node_types)
        .filter(([, count]) => count > 0)
        .sort((a, b) => b[1] - a[1])
    : [];

  return (
    <div className="min-h-screen bg-gray-950 text-white relative">
      {/* Header */}
      <div className="absolute top-0 left-0 right-0 z-20 bg-gray-950/80 backdrop-blur-sm border-b border-gray-800">
        <div className="flex items-center justify-between px-4 py-2">
          <div className="flex items-center gap-3">
            <Link href="/" className="text-gray-400 hover:text-white text-sm">
              &larr; Home
            </Link>
            <h1 className="text-sm font-bold">Knowledge Graph</h1>
            <span className="text-xs text-gray-500">
              {filteredGraph.nodes.length} nodes &middot;{" "}
              {filteredGraph.links.length} edges &middot;{" "}
              {zoomLevel.toFixed(1)}x
            </span>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => { hasZoomedToFit.current = false; setView3D((p) => !p); }}
              className={`px-2.5 py-1 rounded-lg text-xs font-semibold border transition-colors cursor-pointer ${
                view3D
                  ? "bg-blue-600 border-blue-500 text-white"
                  : "bg-gray-800 border-gray-700 text-gray-400 hover:text-white"
              }`}
            >
              {view3D ? "3D" : "2D"}
            </button>
            <input
              type="text"
              placeholder="Search nodes..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-1 text-xs text-white w-48 focus:outline-none focus:border-blue-500"
            />
          </div>
        </div>
      </div>

      {/* Node type filters — left panel */}
      <div className={`absolute top-10 left-0 z-20 bg-gray-900/90 backdrop-blur-sm border-r border-gray-800 p-2 transition-all ${legendCollapsed ? "" : "bottom-0 w-48 overflow-y-auto"}`}>
        <button
          onClick={() => setLegendCollapsed((p) => !p)}
          className="flex items-center justify-center cursor-pointer w-6 h-6 rounded hover:bg-gray-800"
          title={legendCollapsed ? "Show legend" : "Hide legend"}
        >
          <span className="text-gray-400 text-xs">{legendCollapsed ? "\u25B6" : "\u25C0"}</span>
        </button>
        {!legendCollapsed && <>
        <div className="flex items-center justify-between mb-2 mt-1">
          <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wide">
            Node Types
          </p>
          <button
            onClick={() => {
              const allTypes = nodeTypes.map(([t]) => t);
              const allHidden = allTypes.every((t) => hiddenTypes.has(t));
              setHiddenTypes(allHidden ? new Set() : new Set(allTypes));
            }}
            className="text-[10px] text-blue-400 hover:text-blue-300 cursor-pointer"
          >
            {nodeTypes.every(([t]) => !hiddenTypes.has(t)) ? "Deselect All" : "Select All"}
          </button>
        </div>
        {nodeTypes.map(([type, count]) => {
          const typeDef = graphData?.nodes.find((n) => n.type === type);
          const color = typeDef?.color || "#999";
          const hidden = hiddenTypes.has(type);
          return (
            <button
              key={type}
              onClick={() => toggleType(type)}
              className={`flex items-center gap-2 w-full text-left px-2 py-1 rounded text-xs cursor-pointer hover:bg-gray-800 transition-colors ${
                hidden ? "opacity-60" : "opacity-100"
              }`}
            >
              <input
                type="checkbox"
                checked={!hidden}
                readOnly
                className="w-3 h-3 rounded accent-blue-500 pointer-events-none flex-shrink-0"
              />
              <span
                className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                style={{ backgroundColor: color }}
              />
              <span className="truncate">
                {NODE_TYPE_LABELS[type] || type}
              </span>
              <span className="text-gray-600 ml-auto">{count}</span>
            </button>
          );
        })}

        {/* Edge legend */}
        <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wide mt-4 mb-2">
          Edge Types
        </p>
        {graphData &&
          Object.entries(graphData.meta.edge_verbs)
            .filter(([, c]) => c > 0)
            .sort((a, b) => b[1] - a[1])
            .map(([verb, count]) => (
              <div key={verb} className="flex items-center gap-2 px-2 py-0.5 text-xs">
                <span
                  className="w-4 h-0.5 flex-shrink-0"
                  style={{ backgroundColor: VERB_COLORS[verb] || "#6b7280" }}
                />
                <span className="text-gray-400 truncate capitalize">
                  {verb.replace(/_/g, " ")}
                </span>
                <span className="text-gray-600 ml-auto">{count}</span>
              </div>
            ))}
        </>}
      </div>

      {/* Selected node detail — right panel */}
      {selectedNode && (
        <div className="absolute top-12 right-2 z-20 bg-gray-900/95 backdrop-blur-sm rounded-xl border border-gray-800 p-4 w-72 max-h-[70vh] overflow-y-auto">
          <div className="flex items-start justify-between mb-3">
            <div>
              <p className="text-xs font-bold text-gray-500 uppercase">
                {NODE_TYPE_LABELS[selectedNode.type] || selectedNode.type}
              </p>
              <p className="text-sm font-semibold mt-0.5">
                {selectedNode.label}
              </p>
            </div>
            <button
              onClick={() => {
                setSelectedNode(null);
                setHighlightNodes(new Set());
                setHighlightEdges(new Set());
              }}
              className="text-gray-500 hover:text-white text-lg leading-none"
            >
              &times;
            </button>
          </div>

          <div className="space-y-1.5 text-xs text-gray-400">
            {selectedNode.party && (
              <p>
                <span className="text-gray-600">Party:</span>{" "}
                {selectedNode.party}
              </p>
            )}
            {selectedNode.constituency && (
              <p>
                <span className="text-gray-600">Constituency:</span>{" "}
                {selectedNode.constituency.replace(/_/g, " ")}
              </p>
            )}
            {selectedNode.category && (
              <p>
                <span className="text-gray-600">Category:</span>{" "}
                {selectedNode.category}
              </p>
            )}
            {selectedNode.status && (
              <p>
                <span className="text-gray-600">Status:</span>{" "}
                {selectedNode.status}
              </p>
            )}
            {selectedNode.goal_name && (
              <p>
                <span className="text-gray-600">Goal:</span>{" "}
                {selectedNode.goal_name}
              </p>
            )}
            {selectedNode.state && (
              <p>
                <span className="text-gray-600">State:</span>{" "}
                {selectedNode.state.replace(/_/g, " ")}
              </p>
            )}
            {selectedNode.period && (
              <p>
                <span className="text-gray-600">Period:</span>{" "}
                {selectedNode.period}
              </p>
            )}
            {selectedNode.assets != null && (
              <p>
                <span className="text-gray-600">Assets:</span>{" "}
                {selectedNode.assets}
              </p>
            )}
            {selectedNode.criminal_cases != null && selectedNode.criminal_cases > 0 && (
              <p>
                <span className="text-gray-600">Criminal cases:</span>{" "}
                {selectedNode.criminal_cases}
              </p>
            )}
            {selectedNode.candidate_count != null && (
              <p>
                <span className="text-gray-600">Candidates:</span>{" "}
                {selectedNode.candidate_count}
              </p>
            )}
          </div>

          {/* Connected edges */}
          <div className="mt-3 pt-3 border-t border-gray-800">
            <p className="text-[10px] font-bold text-gray-600 uppercase mb-1.5">
              Connections
            </p>
            {graphData &&
              (() => {
                const edges = graphData.edges.filter((e) => {
                  const src =
                    typeof e.source === "string" ? e.source : e.source.id;
                  const tgt =
                    typeof e.target === "string" ? e.target : e.target.id;
                  return src === selectedNode.id || tgt === selectedNode.id;
                });
                const byVerb: Record<string, number> = {};
                for (const e of edges) {
                  byVerb[e.verb] = (byVerb[e.verb] || 0) + 1;
                }
                return Object.entries(byVerb)
                  .sort((a, b) => b[1] - a[1])
                  .map(([verb, count]) => (
                    <div
                      key={verb}
                      className="flex items-center gap-2 text-xs text-gray-400 py-0.5"
                    >
                      <span
                        className="w-3 h-0.5"
                        style={{
                          backgroundColor: VERB_COLORS[verb] || "#6b7280",
                        }}
                      />
                      <span className="capitalize">{verb.replace(/_/g, " ")}</span>
                      <span className="text-gray-600 ml-auto">{count}</span>
                    </div>
                  ));
              })()}
          </div>
        </div>
      )}

      {/* Time slider — bottom bar */}
      <div className="absolute bottom-0 left-0 right-0 z-20 bg-gray-900/90 backdrop-blur-sm border-t border-gray-800 px-6 py-3">
        <div className="flex items-center gap-4 max-w-3xl mx-auto">
          <span className="text-[10px] font-bold text-gray-500 uppercase tracking-wide flex-shrink-0">
            Timeline
          </span>
          <div className="flex-1 relative">
            <input
              type="range"
              min={MIN_YEAR}
              max={MAX_YEAR}
              value={timeRange}
              onChange={(e) => setTimeRange(parseInt(e.target.value))}
              className="w-full h-1.5 bg-gray-700 rounded-full appearance-none cursor-pointer accent-blue-500"
            />
            {/* Tick marks */}
            <div className="flex justify-between mt-1">
              {TIME_MARKERS.map((m) => (
                <button
                  key={m.year}
                  onClick={() => setTimeRange(m.year)}
                  className={`text-[9px] cursor-pointer transition-colors ${
                    timeRange >= m.year ? "text-blue-400 font-bold" : "text-gray-600"
                  }`}
                >
                  {m.year}
                </button>
              ))}
            </div>
          </div>
          <span className="text-xs font-bold text-blue-400 flex-shrink-0 w-16 text-right">
            ≤ {timeRange}
          </span>
        </div>
      </div>

      {/* Graph canvas */}
      <div className="w-full h-screen pt-10 pb-16">
        {/* eslint-disable @typescript-eslint/no-explicit-any */}
        {view3D ? (
          <ForceGraph3D
            ref={graphRef}
            graphData={filteredGraph}
            nodeId="id"
            linkSource="source"
            linkTarget="target"
            nodeColor={(node: any) => {
              const n = node as GraphNode;
              if (highlightNodes.size > 0 && !highlightNodes.has(n.id)) return "#ffffff08";
              return n.color || "#999";
            }}
            nodeVal={(node: any) => {
              const n = node as GraphNode;
              return (NODE_SIZE[n.type] || 3) * 0.5;
            }}
            nodeLabel={(node: any) => {
              const n = node as GraphNode;
              return `${NODE_TYPE_LABELS[n.type] || n.type}: ${n.label}`;
            }}
            linkColor={(link: any) => {
              const verb = link.verb || "";
              if (highlightEdges.size > 0) {
                const src = typeof link.source === "string" ? link.source : link.source?.id;
                const tgt = typeof link.target === "string" ? link.target : link.target?.id;
                if (!highlightEdges.has(`${src}→${tgt}`)) return "#ffffff06";
              }
              return VERB_COLORS[verb] || "#6b7280";
            }}
            linkWidth={(link: any) => {
              const verb = link.verb || "";
              return ["targets_goal", "measured_by", "influences", "promised", "allied_with", "won"].includes(verb) ? 1.5 : 0.3;
            }}
            linkOpacity={0.4}
            onNodeClick={handleNodeClick as any}
            onBackgroundClick={handleBackgroundClick}
            enableNodeDrag={true}
            cooldownTicks={50}
            warmupTicks={0}
            d3VelocityDecay={0.6}
            d3AlphaDecay={0.05}
            d3AlphaMin={0.01}
            backgroundColor="#030712"
            onEngineStop={() => {
              if (!hasZoomedToFit.current && graphRef.current) {
                hasZoomedToFit.current = true;
                graphRef.current.zoomToFit(400, 60);
              }
            }}
          />
        ) : (
          <ForceGraph2D
            ref={graphRef}
            graphData={filteredGraph}
            nodeCanvasObject={paintNode as any}
            linkCanvasObject={paintLink as any}
            onNodeClick={handleNodeClick as any}
            onBackgroundClick={handleBackgroundClick}
            onZoom={(transform: { k: number }) => setZoomLevel(transform.k)}
            nodeId="id"
            linkSource="source"
            linkTarget="target"
            enableNodeDrag={true}
            enableZoomInteraction={true}
            enablePanInteraction={true}
            cooldownTicks={50}
            warmupTicks={0}
            d3VelocityDecay={0.6}
            d3AlphaDecay={0.05}
            d3AlphaMin={0.01}
            linkDirectionalArrowLength={0}
            backgroundColor="#030712"
            minZoom={0.1}
            maxZoom={15}
            onEngineStop={() => {
              if (!hasZoomedToFit.current && graphRef.current) {
                hasZoomedToFit.current = true;
                graphRef.current.zoomToFit(400, 60);
              }
            }}
          />
        )}
        {/* eslint-enable @typescript-eslint/no-explicit-any */}
      </div>
    </div>
  );
}
