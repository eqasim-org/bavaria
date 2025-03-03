import geopandas as gpd
import pandas as pd
import gzip
import xml.etree.ElementTree as ET
import geopandas as gpd
from shapely.geometry import LineString
import os

import shapely.wkt as wkt


# Function to parse XML and convert to DataFrame
def parse_network_xml_gz(file_path):
    with gzip.open(file_path, 'rb') as f:
        tree = ET.parse(f)
        root = tree.getroot()
        
        data = []
        for element in root.findall(".//node"):
            node_data = {
                'id': element.get('id'),
                'x': float(element.get('x')),
                'y': float(element.get('y'))
            }
            data.append(node_data)
        
        df = pd.DataFrame(data)
        return df
    
# Function to parse nodes and create a dictionary for quick lookup
def parse_nodes(file_path):
    with gzip.open(file_path, 'rb') as f:
        tree = ET.parse(f)
        root = tree.getroot()
        
        nodes = {}
        nodes_element = root.find("nodes")
        if nodes_element:
            for element in nodes_element.findall("node"):
                node_id = element.attrib['id']
                x, y = float(element.attrib['x']), float(element.attrib['y'])
                nodes[node_id] = (x, y)
        return nodes

# Function to parse edges (links) and add geometry using the nodes' coordinates
def parse_edges(file_path, nodes):
    with gzip.open(file_path, 'rb') as f:
        tree = ET.parse(f)
        root = tree.getroot()
        
        data = []
        links_element = root.find("links")
        if links_element:
            for element in links_element.findall("link"):
                link_data = element.attrib
                from_node = nodes.get(link_data['from'])
                to_node = nodes.get(link_data['to'])
                if from_node and to_node:
                    link_data['geometry'] = LineString([from_node, to_node])
                data.append(link_data)
        df = pd.DataFrame(data)
        return df
    
# Function to convert a DataFrame back to XML format
def dataframe_to_xml(df, nodes_dict):
    root = ET.Element("network")
    
    # Add attributes
    attributes_elem = ET.SubElement(root, "attributes")
    attribute_dict = {"name": "coordinateReferenceSystem", "class": "java.lang.String"}
    ET.SubElement(attributes_elem, "attribute", attrib=attribute_dict).text = "Atlantis"
    
    # Add nodes
    nodes_elem = ET.SubElement(root, "nodes")
    for node_id, coords in nodes_dict.items():
        ET.SubElement(nodes_elem, "node", id=node_id, x=str(coords[0]), y=str(coords[1]))
    
    # Add links
    links_elem = ET.SubElement(root, "links")
    for _, row in df.iterrows():
        link_attributes = {
            'id': row['id'],
            'from': row['from'],
            'to': row['to'],
            'length': str(row['length']),
            'freespeed': str(row['freespeed']),
            'capacity': str(row['capacity']),
            'permlanes': row['permlanes'],
            'oneway': row['oneway'],
            'modes': row['modes'],
            # 'district': row['district']
        }
        # Replace "inf" with "'Infinity" in the attributes
        for key, value in link_attributes.items():
            if value == "inf":
                link_attributes[key] = "Infinity"
        
        ET.SubElement(links_elem, "link", **link_attributes)
    
    return ET.ElementTree(root)

# Function to write XML to a compressed .gz file
def write_xml_to_gz(xml_tree, file_path):
    xml_declaration = b'<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE network SYSTEM "http://www.matsim.org/files/dtd/network_v2.dtd">\n'
    xml_str = xml_declaration + ET.tostring(xml_tree.getroot(), encoding='utf-8', method='xml')
    xml_str = xml_str.replace(b"inf", b"'Infinity")  # Replace "inf" with "'Infinity"
    with gzip.open(file_path, 'wb') as f:
        f.write(xml_str)
        
# Function to read and convert CSV.GZ to GeoDataFrame
def read_output_links(folder):
    file_path = os.path.join(folder, 'output_links.csv.gz')
    if os.path.exists(file_path):
        # Read the CSV file with the correct delimiter
        df = pd.read_csv(file_path, delimiter=';')
        
        # Convert the 'geometry' column to actual geometrical data
        df['geometry'] = df['geometry'].apply(wkt.loads)
        
        # Create a GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry='geometry')
        return gdf
    else:
        return None
    
# Function to read and convert CSV.GZ to GeoDataFrame
def read_network_data(folder):
    file_path = os.path.join(folder, 'output_links.csv.gz')
    if os.path.exists(file_path):
        # Read the CSV file with the correct delimiter
        df = pd.read_csv(file_path, delimiter=';')
        
        # Convert the 'geometry' column to actual geometrical data
        df['geometry'] = df['geometry'].apply(wkt.loads)
        
        # Create a GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry='geometry')
        return gdf
    else:
        return None
    
# Funktion zur Überprüfung, ob eine Teilmenge verbunden ist
def is_connected(subset, neighbours):
    if not subset:
        return True
    visited = set()

    def dfs(node):
        stack = [node]
        while stack:
            current = stack.pop()
            if current not in visited:
                visited.add(current)
                stack.extend([n for n in neighbours[current] if n in subset and n not in visited])

    dfs(next(iter(subset)))
    return visited == subset
    
    