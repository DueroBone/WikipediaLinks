import json
import networkx as nx
import plotly.graph_objects as go


# === Step 1: Load JSONL file into a graph ===
def load_graph(jsonl_path: str) -> nx.DiGraph:
    G = nx.DiGraph()  # directed graph (Page -> Links)
    with open(jsonl_path, "r", encoding="utf-8") as f:
        lineNum = 0
        for line in f:
            entry = json.loads(line)
            page, links = next(iter(entry.items()))
            for link in links:
                G.add_edge(page, link)
            lineNum += 1
    return G


# === Step 2: Basic analysis ===
def analyze_graph(G: nx.DiGraph):
    print("Number of nodes:", G.number_of_nodes())
    print("Number of edges:", G.number_of_edges())

    # Degree centrality (importance of nodes)
    centrality = nx.degree_centrality(G)
    top5 = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:5]
    print("Top 5 nodes by degree centrality:")
    for node, score in top5:
        print(f"  {node}: {score:.4f}")


# === Step 3: Visualization with Plotly ===
def visualize_graph(G: nx.DiGraph, max_nodes=100):
    # Optionally: limit graph size for visualization
    if max_nodes < 0:
        H = G
    elif G.number_of_nodes() > max_nodes:
        H = G.subgraph(list(nx.nodes(G))[:max_nodes])
    else:
        H = G

    pos = nx.spring_layout(H, seed=42)  # layout positions

    # Extract edges for Plotly
    edge_x, edge_y = [], []
    for src, dst in H.edges():
        x0, y0 = pos[src]
        x1, y1 = pos[dst]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=0.5, color="#888"),
        hoverinfo="none",
        mode="lines",
    )

    # Extract nodes
    node_x, node_y, node_text = [], [], []
    for node in H.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_text.append(node)

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers+text",
        text=node_text,
        textposition="top center",
        hoverinfo="text",
        marker=dict(
            showscale=True,
            colorscale="YlGnBu",
            size=10,
            # color=[H.degree for n in H.nodes()],
            color=[H.degree(n) for n in H.nodes()],
            # color="#FFFFFF",
            colorbar=dict(thickness=15, title="Node Degree", xanchor="left"),
        ),
    )

    fig = go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            title="Wikipedia Graph Visualization",
            # titlefont_size=16,
            showlegend=False,
            hovermode="closest",
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        ),
    )
    fig.show()


# === Example usage ===
if __name__ == "__main__":
    path = "linksOutput.jsonl"
    G = load_graph(path)
    analyze_graph(G)
    visualize_graph(G)
