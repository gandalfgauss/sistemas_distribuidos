"""
--------------------------------------"WELCOME TO LIBARY pyspark"---------------------------------------------------------
"""


def GraphFrame(vertices, edges):
    """this function receives vertices and edges in graphs, and generates a graph"""
    graph ={}
    for id in range(len(vertices)):
        graph[str(id)] = []

    for edge in edges:
        graph[edge.split()[0]].append(edge.split()[1])
        graph[edge.split()[1]].append(edge.split()[0])

    return graph


""" 
---------------------------------------------------------------------------------------------------------------
"""