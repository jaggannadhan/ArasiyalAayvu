"use client";

import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import dynamic from "next/dynamic";
import Link from "next/link";

// react-force-graph-2d uses canvas and window — must be client-only
const ForceGraph2D = dynamic(() => import("react-force-graph-2d"), {
  ssr: false,
});

const GRAPH_URL =
  "https://storage.googleapis.com/naatunadappu-media/knowledge_graph/latest.json";

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
  // Force graph adds these at runtime
  x?: number;
  y?: number;
}

interface GraphEdge {
  source: string | GraphNode;
  target: string | GraphNode;
  verb: string;
  weight: number;
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
  state: 12,
  district: 4,
  constituency: 2,
  party: 8,
  candidate: 1.5,
  mla: 4,
  manifesto_item: 2.5,
  sdg_goal: 7,
  indicator_plfs: 6,
  indicator_srs: 6,
  indicator_hces: 6,
  indicator_udise: 6,
  indicator_aishe: 6,
  indicator_ncrb: 6,
  indicator_asi: 6,
  indicator_col: 6,
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
  const [highlightNodes, setHighlightNodes] = useState<Set<string>>(new Set());
  const [highlightEdges, setHighlightEdges] = useState<Set<string>>(new Set());
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const graphRef = useRef<any>(null);

  // Fetch graph data
  useEffect(() => {
    fetch(GRAPH_URL)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data: GraphData) => {
        // react-force-graph expects "links" not "edges"
        setGraphData(data);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
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
      return visibleIds.has(src) && visibleIds.has(tgt);
    });
    return { nodes: visibleNodes, links: visibleEdges };
  }, [graphData, hiddenTypes]);

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

  // Node paint
  const paintNode = useCallback(
    (node: GraphNode, ctx: CanvasRenderingContext2D) => {
      const size = NODE_SIZE[node.type] || 3;
      const isHighlighted =
        highlightNodes.size === 0 || highlightNodes.has(node.id);
      const alpha = isHighlighted ? 1 : 0.15;

      ctx.beginPath();
      ctx.arc(node.x || 0, node.y || 0, size, 0, 2 * Math.PI);
      ctx.fillStyle =
        node.color + (alpha < 1 ? Math.round(alpha * 255).toString(16).padStart(2, "0") : "");
      ctx.fill();

      // Label for large nodes when zoomed in
      if (size >= 5) {
        ctx.font = `${Math.max(3, size * 0.8)}px sans-serif`;
        ctx.textAlign = "center";
        ctx.textBaseline = "top";
        ctx.fillStyle = isHighlighted ? "#1f2937" : "#9ca3af";
        ctx.fillText(node.label.slice(0, 25), node.x || 0, (node.y || 0) + size + 1);
      }
    },
    [highlightNodes]
  );

  // Edge paint
  const paintLink = useCallback(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (link: any, ctx: CanvasRenderingContext2D) => {
      const src = link.source;
      const tgt = link.target;
      if (!src?.x || !tgt?.x) return;

      const edgeKey = `${src.id}→${tgt.id}`;
      const isHighlighted =
        highlightEdges.size === 0 || highlightEdges.has(edgeKey);

      ctx.beginPath();
      ctx.moveTo(src.x, src.y);
      ctx.lineTo(tgt.x, tgt.y);
      ctx.strokeStyle = isHighlighted
        ? (VERB_COLORS[link.verb] || "#6b7280")
        : "#e5e7eb20";
      ctx.lineWidth = isHighlighted ? (link.weight || 0.5) * 1.5 : 0.2;
      ctx.stroke();
    },
    [highlightEdges]
  );

  // ─── Render ──────────────────────────────────────────────────────────────

  if (loading) {
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
              {filteredGraph.links.length} edges
            </span>
          </div>
          <input
            type="text"
            placeholder="Search nodes..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-1 text-xs text-white w-48 focus:outline-none focus:border-blue-500"
          />
        </div>
      </div>

      {/* Node type filters — left panel */}
      <div className="absolute top-12 left-2 z-20 bg-gray-900/90 backdrop-blur-sm rounded-xl border border-gray-800 p-3 max-h-[70vh] overflow-y-auto">
        <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wide mb-2">
          Node Types
        </p>
        {nodeTypes.map(([type, count]) => {
          const typeDef = graphData?.nodes.find((n) => n.type === type);
          const color = typeDef?.color || "#999";
          const hidden = hiddenTypes.has(type);
          return (
            <button
              key={type}
              onClick={() => toggleType(type)}
              className={`flex items-center gap-2 w-full text-left px-2 py-1 rounded text-xs transition-colors ${
                hidden
                  ? "opacity-40 hover:opacity-60"
                  : "opacity-100 hover:bg-gray-800"
              }`}
            >
              <span
                className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                style={{ backgroundColor: hidden ? "#555" : color }}
              />
              <span className="truncate">
                {type.replace("indicator_", "").replace("_", " ")}
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
                <span className="text-gray-400 truncate">
                  {verb.replace("_", " ")}
                </span>
                <span className="text-gray-600 ml-auto">{count}</span>
              </div>
            ))}
      </div>

      {/* Selected node detail — right panel */}
      {selectedNode && (
        <div className="absolute top-12 right-2 z-20 bg-gray-900/95 backdrop-blur-sm rounded-xl border border-gray-800 p-4 w-72 max-h-[70vh] overflow-y-auto">
          <div className="flex items-start justify-between mb-3">
            <div>
              <p className="text-xs font-bold text-gray-500 uppercase">
                {selectedNode.type.replace("indicator_", "").replace("_", " ")}
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
                      <span>{verb.replace("_", " ")}</span>
                      <span className="text-gray-600 ml-auto">{count}</span>
                    </div>
                  ));
              })()}
          </div>
        </div>
      )}

      {/* Graph canvas */}
      <div className="w-full h-screen pt-10">
        {/* eslint-disable @typescript-eslint/no-explicit-any */}
        <ForceGraph2D
          ref={graphRef}
          graphData={filteredGraph}
          nodeCanvasObject={paintNode as any}
          linkCanvasObject={paintLink as any}
          onNodeClick={handleNodeClick as any}
          onBackgroundClick={handleBackgroundClick}
          nodeId="id"
          linkSource="source"
          linkTarget="target"
          enableNodeDrag={true}
          enableZoomInteraction={true}
          enablePanInteraction={true}
          cooldownTicks={100}
          d3VelocityDecay={0.3}
          d3AlphaDecay={0.02}
          linkDirectionalArrowLength={0}
          warmupTicks={50}
          backgroundColor="#030712"
        />
        {/* eslint-enable @typescript-eslint/no-explicit-any */}
      </div>
    </div>
  );
}
