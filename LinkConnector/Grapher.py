import networkx as nx

# import plotly.graph_objects as go
import pickle
import gzip
import os
import time


def create_graph(pickle_path: str) -> nx.DiGraph:
    G = nx.DiGraph()  # directed graph (Page -> Links)
    with open(pickle_path, "rb") as f:
        links_list = pickle.load(f)
        for page, links in links_list:
            for link in links:
                G.add_edge(page, link)
    return G


"""
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
            color=[H.degree(n) for n in H.nodes()],  # type: ignore
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
"""


def load_and_save(load_graph, output_path):
    input_path = "enwiki_links_list.pkl"
    # Encapsulated in function so G is not kept in memory
    print("Generating graph...")
    G = load_graph(input_path)
    print("Graph loaded. Saving to disk...")
    with open(output_path, "wb") as f:
        pickle.dump(G, f)


def full_save():
    output_path = "wikipedia_graph.pkl"
    load_and_save(create_graph, output_path)
    print(f"Temp graph saved to {output_path}. Compressing...")

    gzipped_output_path = "wikipedia_graph.pkl.gz"
    with open(output_path, "rb") as f_in:
        with gzip.open(gzipped_output_path, "wb") as f_out:
            f_out.writelines(f_in)
    os.remove(output_path)

    print(f"Gzipped graph saved to {gzipped_output_path}")
    print()


if __name__ == "__main__":
    start_time = time.time()
    G = pickle.load(open("wikipedia_graph.pkl", "rb"))
    print(
        f"Graph loaded from wikipedia_graph.pkl in {time.time() - start_time:.2f} seconds"
    )
    pass
