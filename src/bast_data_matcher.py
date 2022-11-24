### Code to match the BAST traffic count data with the OSM network

import pandas as pd
import numpy as np
import json, glob, utm, requests, time
import lxml.html as lh
from tqdm import tqdm	
from geopy.geocoders import Nominatim
import osmnx as ox
from rtree.index import Index as RTreeIndex
from shapely.geometry import Point

def read_locations(file_name):
	with open(file_name) as f:
		lines = f.readlines()
	X = []
	Y = []
	color = []
	road = []
	place = []
	place_no = []
	for line in lines:
		if line!="\n":
	#         print(line)
			try:
				line_content = line.split(",")
	#             print(line_content)
				X.append(np.float(line_content[1].strip(" \"")))
				Y.append(np.float(line_content[2]))
				color.append(line_content[3])
				road.append(line_content[4].split(":")[0])
				place.append(line_content[4].split(": ")[1])
				place_no.append(int(line_content[4][-5:-1]))
			except Exception as e:
				print(e)
				print(line_content)
				break
	df_bast = pd.DataFrame({"X":X, "Y":Y, "color":color, "road":road, 
			  "place": place, "place_no": place_no})
			#  "verkehr_tag": kfz_verkehr_tag, "schwerverkehr_tag": schwerverkehr_tag})
	return df_bast

def web_bast(df_bast, web_folder):
	for row in tqdm(range(0, len(df_bast))):
		url = "https://www.bast.de/BASt_2017/DE/Verkehrstechnik/Fachthemen/"+\
				"v2-verkehrszaehlung/Aktuell/zaehl_aktuell_node.html?nn=1819516&cms_detail="+\
			str(list(df_bast.place_no)[row])+"&cms_map=0"
		res = requests.get(url)
		html_file = open(web_folder+'/'+str(list(df_bast.place_no)[row])+".json",'w')
		html_file.write(res.text)
		html_file.close()
		time.sleep(4)
	print("Web data received")

def read_web(web_folder):
	'''Reading attributes from the data
	'''
	files = glob.glob(web_folder+'/*.json')
	place_no = []
	direction_1_short = []
	direction_1_long = []
	direction_2_short = []
	direction_2_long = []

	for file in files:
		with open(file) as f:
			lines = f.read().replace('\n', '')
		doc = lh.fromstring(lines)
		place_no.append(int(file.split("/")[-1][:4]))
		tr_elements = doc.xpath('.//td')
		direction_1_long.append(tr_elements[10].text_content())
		direction_2_long.append(tr_elements[11].text_content())
		direction_1_short.append(tr_elements[12].text_content())
		direction_2_short.append(tr_elements[13].text_content())
	direction_mapping = pd.DataFrame({'place_no':place_no, 'direction_1_long':direction_1_long,
			  "direction_1_short": direction_1_short, "direction_2_long": direction_2_long,
			 "direction_2_short": direction_2_short})
	return direction_mapping

def geocode_destinations(df_bast):
	geolocator = Nominatim(user_agent="example app")
	direction_1_long_coord = []
	direction_2_long_coord = []
	direction_1_short_coord = []
	direction_2_short_coord = []
	for i in tqdm(range(36, len(df_bast))):
		try:
			direction_1_long_coord.append(geolocator.geocode(list(df_bast.direction_1_long)[i]).point)
		except:
			direction_1_long_coord.append(0)
		time.sleep(1)
		try:
			direction_2_long_coord.append(geolocator.geocode(list(df_bast.direction_2_long)[i]).point)
		except:
			direction_2_long_coord.append(0)
		time.sleep(1)
		try:
			direction_1_short_coord.append(geolocator.geocode(list(df_bast.direction_1_short)[i].split(" (")[0]).point)
		except:
			direction_1_short_coord.append(0)
		time.sleep(1)
		try:
			direction_2_short_coord.append(geolocator.geocode(list(df_bast.direction_2_short)[i].split(" (")[0]).point)
		except:
			direction_2_short_coord.append(0)
		time.sleep(1)
	df_bast['direction_1_long_coord']= direction_1_long_coord
	df_bast['direction_2_long_coord']= direction_2_long_coord
	df_bast['direction_1_short_coord']= direction_1_short_coord
	df_bast['direction_2_short_coord']= direction_2_short_coord
	return df_bast

def dms2dd(degrees, minutes, seconds, direction):
	dd = float(degrees) + float(minutes)/60 + float(seconds)/(60*60);
	if direction == 'W' or direction == 'S':
		dd *= -1
	return dd;

def get_dgmsd(x, i):
	dg = int(x.split(", ")[i].split(" ")[0])
	m = int(x.split(", ")[i].split(" ")[1][:-1])
	s = float(x.split(", ")[i].split(" ")[2][:-1])
	d = x.split(", ")[i].split(" ")[3]
	return dg, m, s, d

def convert_to_decimals(df_bast):
	df_bast['direction_1_long_coord_lat'] = df_bast['direction_1_long_coord'].apply(lambda x: dms2dd(*get_dgmsd(x, 0)) if x!='0' else 0)
	df_bast['direction_1_long_coord_lon'] = df_bast['direction_1_long_coord'].apply(lambda x: dms2dd(*get_dgmsd(x, 1))  if x!='0' else 0)
	df_bast['direction_2_long_coord_lat'] = df_bast['direction_2_long_coord'].apply(lambda x: dms2dd(*get_dgmsd(x, 0)) if x!='0' else 0)
	df_bast['direction_2_long_coord_lon'] = df_bast['direction_2_long_coord'].apply(lambda x: dms2dd(*get_dgmsd(x, 1))  if x!='0' else 0)
	df_bast['direction_1_short_coord_lat'] = df_bast['direction_1_short_coord'].apply(lambda x: dms2dd(*get_dgmsd(x, 0)) if x!='0' else 0)
	df_bast['direction_1_short_coord_lon'] = df_bast['direction_1_short_coord'].apply(lambda x: dms2dd(*get_dgmsd(x, 1))  if x!='0' else 0)
	df_bast['direction_2_short_coord_lat'] = df_bast['direction_2_short_coord'].apply(lambda x: dms2dd(*get_dgmsd(x, 0)) if x!='0' else 0)
	df_bast['direction_2_short_coord_lon'] = df_bast['direction_2_short_coord'].apply(lambda x: dms2dd(*get_dgmsd(x, 1))  if x!='0' else 0)
	return df_bast

def get_nearest_edge(df_bast, Gp):
	'''Obtain nearest edge to the node
	'''
	rtree = RTreeIndex()
	geoms = ox.utils_graph.graph_to_gdfs(Gp, nodes=False)["geometry"]
	for pos, bounds in enumerate(geoms.bounds.values):
		rtree.insert(pos, bounds)
	node_1_from = []
	node_2_from = []
	node_1_to = []
	node_2_to = []
	distance_1 = []
	distance_2 = []
	for i in tqdm(range(0, len(df_bast))):
		point = np.float(df_bast.loc[i,'lat']), np.float(df_bast.loc[i,'lon'])
		point_geom_proj, crs = ox.projection.project_geometry(Point(reversed(point)), to_crs=Gp.graph['crs'])
		X, Y = point_geom_proj.x, point_geom_proj.y
		# use r-tree to find possible nearest neighbors, one point at a time,
		# then minimize euclidean distance from point to the possible matches
		X = [X]
		Y = [Y]
		for xy in zip(X, Y):
			dists = geoms.iloc[list(rtree.nearest(xy, num_results=2))].distance(Point(xy))
	#         ne_dist.append((dists.idxmin(), dists.min()))
			temp = dists.sort_values().reset_index().drop_duplicates().reset_index()
			node_1_from.append(temp.loc[0,'u'])
			node_1_to.append(temp.loc[0,'v'])
			distance_1.append(temp.loc[0,0])
			try:
				node_2_from.append(temp.loc[1,'u'])
				node_2_to.append(temp.loc[1,'v'])
				distance_2.append(temp.loc[1,0])
			except:
				print(temp)
				node_2_from.append(0)
				node_2_to.append(0)
				distance_2.append(0)

	df_bast['node_a_from'] = node_1_from
	df_bast['node_a_to'] = node_1_to
	df_bast['distance_a'] = distance_1
	df_bast['node_b_from'] = node_2_from
	df_bast['node_b_to'] = node_2_to
	df_bast['distance_b'] = distance_2
	return df_bast

			
def decide_edge_direction(df_bast, Gp):
	'''Heuristic to match the directional flow the OSM link
	'''
	a_direc = []
	b_direc = []
	node_a_lat_from = []
	node_a_lon_from = []
	node_b_lat_from = []
	node_b_lon_from = []
	node_a_lat_to = []
	node_a_lon_to = []
	node_b_lat_to = []
	node_b_lon_to = []

	for i in range(0, len(df_bast)):
		point_end_a_edge = Gp.nodes[df_bast.loc[i,'node_a_to']]['y'], Gp.nodes[df_bast.loc[i,'node_a_to']]['x']
		point_end_b_edge = Gp.nodes[df_bast.loc[i,'node_b_to']]['y'], Gp.nodes[df_bast.loc[i,'node_b_to']]['x']

		a_lat, a_lon = utm.to_latlon(point_end_a_edge[1], point_end_a_edge[0], zone_number=32, zone_letter="N")
		node_a_lat_to.append(a_lat)
		node_a_lon_to.append(a_lon)
		
		b_lat, b_lon = utm.to_latlon(point_end_b_edge[1], point_end_b_edge[0], zone_number=32, zone_letter="N")
		node_b_lat_to.append(b_lat)
		node_b_lon_to.append(b_lon)
		
		point_dest_1 = df_bast.loc[i,'direction_1_long_coord_lat'], df_bast.loc[i,'direction_1_long_coord_lon']
		point_geom_proj_1, crs = ox.projection.project_geometry(Point(reversed(point_dest_1)), to_crs=Gp.graph['crs'])
		d_a1 = ox.distance.euclidean_dist_vec(point_end_a_edge[0], point_end_a_edge[1], point_geom_proj_1.y, point_geom_proj_1.x)
		d_b1 = ox.distance.euclidean_dist_vec(point_end_b_edge[0], point_end_b_edge[1], point_geom_proj_1.y, point_geom_proj_1.x)
		
		if d_a1<d_b1:
			a_direc.append(1)
		else:
			a_direc.append(2)
			
	for i in range(0, len(df_bast)):
		point_end_a_edge = Gp.nodes[df_bast.loc[i,'node_a_from']]['y'], Gp.nodes[df_bast.loc[i,'node_a_from']]['x']
		point_end_b_edge = Gp.nodes[df_bast.loc[i,'node_b_from']]['y'], Gp.nodes[df_bast.loc[i,'node_b_from']]['x']

		a_lat, a_lon = utm.to_latlon(point_end_a_edge[1], point_end_a_edge[0], zone_number=32, zone_letter="N")
		node_a_lat_from.append(a_lat)
		node_a_lon_from.append(a_lon)
		
		b_lat, b_lon = utm.to_latlon(point_end_b_edge[1], point_end_b_edge[0], zone_number=32, zone_letter="N")
		node_b_lat_from.append(b_lat)
		node_b_lon_from.append(b_lon)
	
	df_bast['a_direc'] = a_direc
	df_bast['b_direc'] = df_bast['a_direc'].apply(lambda x: 1 if x==2 else 2)
	df_bast['node_a_to_lat']  = node_a_lat_to
	df_bast['node_a_to_lon']  = node_a_lon_to
	df_bast['node_b_to_lat']  = node_b_lat_to
	df_bast['node_b_to_lon']  = node_b_lon_to

	df_bast['node_a_from_lat']  = node_a_lat_from
	df_bast['node_a_from_lon']  = node_a_lon_from
	df_bast['node_b_from_lat']  = node_b_lat_from
	df_bast['node_b_from_lon']  = node_b_lon_from
	return df_bast

if __name__ == "__main__":


	# specify the paths to the I/O files
	BAST_LOCATION_FILE 		= '../data/BAST/locations.txt'
	SAVE_DETECTOR_FILE 		= '../scenario_munich/bast_detectors.csv'
	RAW_WEB_LOCATION 	= '../data/BAST/web'
	FINAL_OUTPUT 			= '../data/bast_detectors_munich.csv'

	# BBOX for the location of interest
	AREA_BBOX = [48.2735, 48.0164,11.778,11.37]

	# This part of the code can be disabled after first run to avoid repeating web data requests
	df_bast = read_locations(file_name=BAST_LOCATION_FILE)
	df_bast['lat'] = df_bast.apply(lambda x: utm.to_latlon(x.X, x.Y, zone_number=32, zone_letter="N")[0], axis=1)
	df_bast['lon'] = df_bast.apply(lambda x: utm.to_latlon(x.X, x.Y, zone_number=32, zone_letter="N")[1], axis=1)
	df_bast.to_csv(SAVE_DETECTOR_FILE, index=None)
	df_bast = pd.read_csv(SAVE_DETECTOR_FILE, index=None)
	web_bast(df_bast, web_folder=RAW_WEB_LOCATION)

	# Read Web data and create interim dataframe
	direction_mapping = read_web(web_folder=RAW_WEB_LOCATION)
	df_bast = pd.merge(left = df_bast, right = direction_mapping, left_on="place_no", right_on="place_no")
	df_bast.to_csv(SAVE_DETECTOR_FILE, index=None)

	df_bast = convert_to_decimals(df_bast)

	# read munich OSM network
	munich_network = ox.graph_from_bbox(AREA_BBOX[0], 
										AREA_BBOX[1], 
										AREA_BBOX[2], 
										AREA_BBOX[3], #network_type='drive',
										custom_filter='["highway"~"motorway|trunk|primary"]')
	Gp = ox.project_graph(munich_network)

	df_bast = get_nearest_edge(df_bast, Gp)

	# filter for the area, in this case Munich Outer Ring
	df_bast = df_bast[df_bast.lat<AREA_BBOX[0]]
	df_bast = df_bast[df_bast.lat>AREA_BBOX[1]]
	df_bast = df_bast[df_bast.lon>AREA_BBOX[2]]
	df_bast = df_bast[df_bast.lon<AREA_BBOX[3]]
	df_bast.reset_index(inplace=True, drop=True)

	df_bast = decide_edge_direction(df_bast, Gp)
	df_bast.to_csv(FINAL_OUTPUT, index=None)